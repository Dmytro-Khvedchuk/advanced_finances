

def HFT_signal_generator(data, is_in_trade = False, last_price = 0, last_position = ""):
    bids = data["bids"]
    asks = data["asks"]

    bid_volume = sum(float(vol) for _, vol in bids)
    ask_volume = sum(float(vol) for _, vol in asks)

    bid_price = float(bids[0][0])
    ask_price = float(asks[0][0])

    if ask_volume == 0:
        ask_volume = 0.000001
    elif bid_volume == 0:
        bid_volume = 0.000001

    best_price = 0
    signal = None

    if not is_in_trade:
        if bid_volume / ask_volume < 0.85:
            signal = "SELL"
            best_price = bid_price
        elif ask_volume / bid_volume < 0.85:
            signal = "BUY"
            best_price = ask_price
    else:
        if abs(last_price - bid_price) >= 50 or abs(last_price - ask_price) >= 50:
            signal = "OVER"
            if last_position == "SELL":
                best_price = bid_price
            elif last_position == "BUY":
                best_price = ask_price

    print("Best price:", best_price)
    print("Best signal:", signal)

    return best_price, signal

