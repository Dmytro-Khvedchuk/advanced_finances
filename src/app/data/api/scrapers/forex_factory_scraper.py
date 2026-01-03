"""Scrape the Forex Factory economic calendar into structured dataframes."""

from collections.abc import Iterable

import polars as pl
from bs4 import BeautifulSoup
from bs4.element import PageElement, Tag
from cloudscraper import CloudScraper, create_scraper  # type: ignore[reportMissingTypeStubs]
from pydantic import ValidationError
from requests import HTTPError, Response

from src.app.data.api.domain.value_objects import (
    FOREX_FACTORY_IMPACT_MAP, ForexEventTimeType, ForexFactoryCurrencies, ForexFactoryImpact, ForexFactoryImpactIcon,
    ForexFactorySectionClasses, MONTHS
)
from src.app.data.api.presentation.schemas import (
    ForexFactoryDataframeSchema, ForexFactoryEventSchema, ForexFactoryNumericValue
)
from src.app.system.logs import get_logger


logger = get_logger(__name__)


class ForexFactoryScraper:
    """Scrape the Forex Factory calendar and normalize event records."""
    def __init__(self) -> None:
        """Initialize the scraper with headers and Cloudflare cookies."""
        self.base_url: str = "https://www.forexfactory.com/calendar/"                    

        self.scraper: CloudScraper = create_scraper(
            browser={
                "browser": "chrome",
                "platform": "windows",
                "desktop": True,
            }
        )

        self.scraper.headers.update({
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            ),
            "Accept-Language": "en-US,en;q=0.9",
            "Referer": "https://www.forexfactory.com/",
        })

        # REQUIRED: establish Cloudflare cookies
        self.scraper.get("https://www.forexfactory.com/")

    def get_data(self, from_month: int, from_year: int, duration_months: int) -> pl.DataFrame | None:
        """Fetch calendar data for a range of months.

        Args:
            from_month (int): Starting month number (1-12).
            from_year (int): Starting year number (e.g., 2024).
            duration_months (int): Number of months to fetch sequentially.
        """
        logger.info(f"Trying to fetch the data from year: {from_year}, " 
                    f"from month: {from_month}, for {duration_months} months")

        month: int = from_month
        year: int = from_year
        for _ in range(duration_months):
            current_url: str = f"{self.base_url}?month={MONTHS[month]}.{year}"

            data: pl.DataFrame = self.fetch_data_from_url(current_url)
            logger.info(f"Successfully fetched {data.shape} records from the {current_url}.")
            data.write_parquet("123.parquet")
            logger.info(data)

            month += 1
            if month > len(MONTHS):
                month = 1
                year += 1
            
    def fetch_data_from_url(self, url: str) -> pl.DataFrame:  # noqa: PLR0912, PLR0914, PLR0915
        """Fetch and parse calendar data from a Forex Factory URL.

        Args:
            url: Calendar URL for a specific month.

        Returns:
            Parsed calendar data as a Polars DataFrame.

        Raises:
            HTTPError: When the request fails or returns a bad status.
        """
        logger.info(f"Fetching data from the {url}")

        try:
            response: Response = self.scraper.get(url)
            response.raise_for_status()
        except HTTPError:
            logger.error(f"An exception during getting the data by url {url} occurred", exc_info=True)
            raise

        soup: BeautifulSoup = BeautifulSoup(response.text, "lxml")

        table: Tag | None = soup.find("table", class_=ForexFactorySectionClasses.TABLE)

        if not table:
            logger.warning(f"{ForexFactorySectionClasses.TABLE} not found.")
            return pl.DataFrame()  # TODO: add a custom exception.

        year: str = url[-4:]

        new_calendar_days: list[Tag] | None = table.find_all("tr", class_=ForexFactorySectionClasses.NEW_DAY_SEPARATOR)

        parent: Tag | None = new_calendar_days[0].parent
        if not parent:
            return pl.DataFrame()  # TODO: add a custom exception.

        day_rows: list[Tag] | None = parent.find_all("tr")
        day_data: list[list[Tag]] = self.split_by_day_breaker(day_rows)[1:]

        month_data: list[dict] = []

        for index, day in enumerate(new_calendar_days):
            calendar_date_tag: Tag | None = day.find("td", class_=ForexFactorySectionClasses.CALENDAR_DATE)
            date_tag: Tag | None = (
                calendar_date_tag.find("span", class_=ForexFactorySectionClasses.DATE) if calendar_date_tag else None
            )
            date_element_tag: Tag | None = date_tag.find("span") if date_tag else None

            if not date_element_tag or not date_element_tag.contents:
                continue

            date_element: PageElement = date_element_tag.contents[0]
            date: str = f"{date_element} {year}"

            for event in day_data[index]:
                event_time: str | None = None
                current_time_tag: Tag | None = event.find("td", class_=ForexFactorySectionClasses.TIME)
                if current_time_tag:
                    event_time_tag: PageElement | None = self.get_contents(current_time_tag)
                    if event_time_tag is not None:
                        event_time: str | None = str(event_time_tag)

                if event_time is None:
                    continue  # TODO: Add custom exception

                time_type: ForexEventTimeType = self.get_time_type(event_time)
                event_time_value = event_time if time_type == ForexEventTimeType.EXACT else None

                raw_event_time = event_time.strip()

                time_type: ForexEventTimeType = self.get_time_type(raw_event_time)

                event_time_value: str | None = None
                if time_type == ForexEventTimeType.EXACT:
                    event_time_value = raw_event_time

                currency_tag: Tag | None = event.find("td", class_=ForexFactorySectionClasses.CURRENCY)
                currency: PageElement | None = self.get_contents(currency_tag)

                impact_tag: Tag | None = event.find("td", class_=ForexFactorySectionClasses.IMPACT)
                impact: ForexFactoryImpact | None = self.get_impact_value(impact_tag) 

                event_name_tag: Tag | None = event.find("span", class_=ForexFactorySectionClasses.EVENT_TITLE)
                event_name: PageElement | None = self.get_contents(event_name_tag)

                actual_tag: Tag | None = event.find("td", class_=ForexFactorySectionClasses.ACTUAL)
                actual: str | None = str(self.get_values_for_numericals(actual_tag))

                forecast_tag = event.find("td", class_=ForexFactorySectionClasses.FORECAST)
                forecast: str | None = str(self.get_values_for_numericals(forecast_tag))

                previous_tag: Tag | None = event.find("td", class_=ForexFactorySectionClasses.PREVIOUS)
                previous: PageElement | None = None
                if previous_tag:
                    previous_tag = previous_tag.find("span", recursive=True)

                    previous: PageElement | None = self.get_contents(previous_tag)

                try:
                    event_record = ForexFactoryEventSchema(
                        event_date=date,
                        event_time=event_time_value,
                        event_time_type=time_type,
                        currency=ForexFactoryCurrencies(currency),
                        impact=ForexFactoryImpact(impact),
                        name=str(event_name),
                        actual=ForexFactoryNumericValue.parse_str_into_numerical(actual),
                        forecast=ForexFactoryNumericValue.parse_str_into_numerical(forecast),
                        previous=ForexFactoryNumericValue.parse_str_into_numerical(str(previous)),
                    )
                except ValidationError as exc:
                    logger.warning(
                        "Skipping invalid Forex Factory event",
                        extra={
                            "date": date,
                            "raw_time": event_time,
                            "event_name": str(event_name),
                            "currency": str(currency),
                            "url": url,
                            "error": exc.errors(),
                        },
                    )
                    continue

                month_data.append(event_record.model_dump())

        return pl.DataFrame(schema=ForexFactoryDataframeSchema, data=month_data)

    @staticmethod
    def get_impact_value(impact_tag: Tag | None) -> ForexFactoryImpact | None:
        """Resolve the impact enum from a table cell tag.

        Args:
            impact_tag: Tag containing the impact span.

        Returns:
            The mapped impact value, or None if unavailable.
        """
        if impact_tag is None:
            return None

        impact_span: Tag | None = impact_tag.find("span")
        if impact_span is None:
            return None

        classes: Iterable[str] | None = impact_span.get("class")
        if not classes:
            return None

        impact_class: str | None = next(
            (
                cls
                for cls in classes
                if cls.startswith(ForexFactorySectionClasses.IMPACT_COMMON)
            ),
            None,
        )
        if impact_class is None:
            return None

        impact_icon: ForexFactoryImpactIcon | None = ForexFactoryImpactIcon(impact_class)
        return FOREX_FACTORY_IMPACT_MAP.get(impact_icon)

    @staticmethod
    def get_time_type(time_str: str) -> ForexEventTimeType:
        """Classify the event time string into a known time type.

        Args:
            time_str: Raw time label from the calendar.

        Returns:
            The corresponding event time type enum.
        """
        value: str = time_str.strip().lower()

        if value == "all day":
            return ForexEventTimeType.ALL_DAY

        if value == "tentative":
            return ForexEventTimeType.TENTATIVE

        if value.startswith("day"):
            return ForexEventTimeType.MULTI_DAY

        return ForexEventTimeType.EXACT

    @staticmethod
    def get_contents(tag: Tag | None) -> PageElement | None:
        """Return the first child content of a tag, if present.

        Args:
            tag: BeautifulSoup tag to read contents from.

        Returns:
            The first content element, or None when missing.
        """
        if tag:
            value: PageElement | None = tag.contents[0] if tag.contents else None
            return value
        return None
            
    def get_values_for_numericals(self, tag: Tag | None) -> PageElement | None:
        """Extract the numeric value from a cell tag.

        Args:
            tag: BeautifulSoup tag that may contain a nested span.

        Returns:
            The first content of the nested span, or None.
        """
        if tag:
            value_tag: Tag | None = tag.find("span", recursive=True)
            return self.get_contents(value_tag)
        return None

    @staticmethod
    def split_by_day_breaker(rows: list[Tag]) -> list[list[Tag]]:
        """Split table rows into day groups separated by day-breaker rows.

        Args:
            rows: Calendar table rows that may include day-breaker separators.

        Returns:
            Groups of rows per day, excluding the separator rows.
        """
        result: list[list[Tag]] = []
        current: list[Tag] = []

        for row in rows:
            classes: list[str] | str | None = row.get("class")
            if classes is None:
                classes_list: list[str] = []
            elif isinstance(classes, str):
                classes_list = [classes]
            else:
                classes_list = [str(item) for item in classes]

            # check if row is a day-breaker
            if ForexFactorySectionClasses.DAY_BREAKER_SEPARATOR in classes_list:
                # only append non-empty groups
                if current:
                    result.append(current)
                    current = []
            else:
                current.append(row)

        # append last group
        if current:
            result.append(current)

        return result
