from datetime import date, timedelta

def last_trading_day_of_week(current: date, closed_dates: set) -> date | None:
    """Finds the last valid trading day of the current week."""
    friday = current + timedelta(days=(4 - current.weekday()))
    candidate = friday
    while candidate >= (friday - timedelta(days=4)):
        if candidate.weekday() < 5 and candidate not in closed_dates:
            return candidate
        candidate -= timedelta(days=1)
    return None

def wednesday_of_week(current: date) -> date:
    """Returns the Wednesday of the current week."""
    return current + timedelta(days=(2 - current.weekday()))

def get_next_valid_vix_expiration(current_date: date, avail_set: set) -> date | None:
    """Finds the next valid VIX expiration starting from the current_date."""
    candidate = current_date + timedelta(days=1)
    for _ in range(45): # Search limit
        if candidate in avail_set:
            return candidate
        candidate += timedelta(days=1)
    return None

def select_target_expirations(symbol: str, current_date: date, available: list, closed_dates: set) -> list:
    """Selects target expirations (0DTE + Weekly) based on symbol logic."""
    avail_set = set(available)
    targets = set()
    weekly = last_trading_day_of_week(current_date, closed_dates)

    if symbol in ("SPX", "SPXW", "SPY", "QQQ"):
        if current_date in avail_set:
            targets.add(current_date)
        if weekly and weekly != current_date and weekly in avail_set:
            targets.add(weekly)
            
    elif symbol == "VIX":
        wed_this_week = wednesday_of_week(current_date)
        if current_date.weekday() in [0, 1]:
            if wed_this_week in avail_set and wed_this_week not in closed_dates:
                targets.add(wed_this_week)
            else:
                next_exp = get_next_valid_vix_expiration(current_date, avail_set)
                if next_exp: targets.add(next_exp)
        else:
            next_exp = get_next_valid_vix_expiration(current_date, avail_set)
            if next_exp: targets.add(next_exp)

    return sorted(list(targets))