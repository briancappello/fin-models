from .calendar import Calendar
from .store import Store


nyse = Calendar("NYSE")
timeframe_stores: dict[str, Store] = {tf: Store(timeframe=tf) for tf in ["minute", "day"]}

store = timeframe_stores['day']
minute_store = timeframe_stores['minute']
