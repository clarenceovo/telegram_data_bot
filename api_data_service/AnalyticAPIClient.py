import httpx
import pandas as pd

class AnalyticAPIClient:
    def __init__(self, base_url: str='http://18.180.162.113:9191'):
        self.base_url = base_url

    def get_instrument_signal(self, symbol: str, duration: int=365):
        url = f"{self.base_url}/inst/getInstrumentSignal"
        params = {
            'symbol': symbol,
            'duration': duration
        }
        response = httpx.get(url, params=params)
        return pd.DataFrame(response.json()['data'])
