from utils.logger.logger import LoggerWrapper
from bs4 import BeautifulSoup
from utils.global_variables.GLOBAL_VARIABLES import MONTHS, IMPACT_MAP
import cloudscraper


class ForexFactoryScraper:
    def __init__(self, log_level: int = 10):
        self.logger = LoggerWrapper(
            name="Forex Factory Scraper Module", level=log_level
        )

        self.base_url = "https://www.forexfactory.com/calendar/"
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/117.0.0.0 Safari/537.36",
            "Accept-Language": "en-US,en;q=0.9",
        }

        self.scraper = cloudscraper.create_scraper()

    def get_data(self, from_month: int, from_year: int, duration_months: int):
        month = from_month
        year = from_year
        for _ in range(duration_months):
            current_url = f"{self.base_url}?month={MONTHS[month]}.{year}"

            data = self.fetch_data_from_url(current_url)
            print(data)

            month += 1
            if month > 12:
                month = 1
                year += 1
            

    def fetch_data_from_url(self, url: str):
        response = self.scraper.get(url)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.text, "lxml")

        table = soup.find("table", class_="calendar__table")

        if table is None:
            print("calendar__table not found")
            return []

        year = url[-4:]

        new_calendar_days = table.find_all("tr", class_="calendar__row--new-day")


        day_data = self.split_by_day_breaker(new_calendar_days[0].parent.find_all("tr"))[1:]       

        month_data = {}

        for index, day in enumerate(new_calendar_days):
            # find date
            date = day.find("td", class_="calendar__date")
            date = date.find("span", class_="date")
            date = date.find("span").contents[0]
            date = f"{date} {year}"

            day_events = {}

            event_time = None

            for event in day_data[index]:
                current_time = event.find("td", class_="calendar__time")
                if current_time:
                    event_time = current_time.contents[0] if current_time.contents else event_time

                currency = event.find("td", class_="calendar__currency")
                if currency:
                    currency = currency.contents[0] if currency.contents else None


                classes = event.find("span").get("class", [])
                impact_class = next((c for c in classes if c.startswith("icon--ff-impact-")), None)
                # fix impact is None, make it into a table
                impact = IMPACT_MAP.get(impact_class)

                event_name = event.find("span", class_="calendar__event-title")
                if event_name:
                    event_name = event_name.contents[0] if event_name.contents else None

                actual = event.find("td", class_="calendar__actual")
                if actual:
                    actual = actual.find("span")
                    if actual:
                        actual = actual.contents[0] if actual.contents else None

                forecast = event.find("td", class_="calendar__forecast")
                if forecast:
                    forecast = forecast.find("span")
                    if forecast:
                        forecast = forecast.contents[0] if forecast.contents else None

                previous_tag = event.find("td", class_="calendar__previous")
                if previous_tag:
                    previous_tag = previous_tag.find("span", recursive=True)

                    if previous_tag is None:
                        previous = None
                    elif not previous_tag.contents:
                        previous = None
                    else:
                        previous = previous_tag.contents[0]

                day_events.update({
                    "date": date,
                    "time": event_time,
                    "currency": currency,
                    "impact": impact,
                    "name": event_name,
                    "actual": actual,
                    "forecast": forecast,
                    "previous": previous
                })

                print(day_events)

        return []

    @staticmethod
    def split_by_day_breaker(rows):
        result = []
        current = []

        for row in rows:
            classes = row.get("class", [])

            # check if row is a day-breaker
            if ("calendar__row--day-breaker" in classes):
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