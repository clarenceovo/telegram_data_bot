import unittest

import numpy as np
import pandas as pd

from analytics.volume_profile import analyze_volume_profile, VolumeProfileError


def bars(sessions=60):
    rng = np.random.default_rng(28)
    days = pd.bdate_range("2026-01-01", periods=sessions, tz="Asia/Hong_Kong")
    index = pd.DatetimeIndex([day + pd.Timedelta(hours=10, minutes=i)
                              for day in days for i in range(40)])
    values = np.tile(np.repeat([90.0, 100.0, 110.0, 120.0], 10), sessions)
    prices = values + rng.normal(0, 0.5, len(index))
    prices[-1] = 105
    return pd.DataFrame({"close": prices, "volume": np.ones(len(index))}, index=index)


class VolumeProfileTests(unittest.TestCase):
    def test_multimodal_zones_volume_and_poc(self):
        data = bars()
        result = analyze_volume_profile(data)
        self.assertEqual(len(result.supports), 2)
        self.assertEqual(len(result.resistances), 2)
        self.assertAlmostEqual(result.volume_shares.sum(), 1)
        self.assertEqual(result.sessions, 60)
        self.assertEqual(result.bar_count, 2400)
        self.assertAlmostEqual(result.supports[0].center, 100, delta=0.5)
        self.assertAlmostEqual(result.resistances[0].center, 110, delta=0.5)
        counts, edges = np.histogram(data.close, bins=48, weights=data.volume)
        peak_bin = int(np.argmax(counts))
        self.assertAlmostEqual(result.poc, (edges[peak_bin] + edges[peak_bin + 1]) / 2)
        for zone in result.supports + result.resistances:
            self.assertLess(zone.lower, zone.center)
            self.assertLess(zone.center, zone.upper)
            self.assertTrue(zone.stable)
            inside = data.close.between(zone.lower, zone.upper)
            self.assertAlmostEqual(zone.volume_share, data.loc[inside, "volume"].sum() / data.volume.sum())
        for zone in result.supports:
            self.assertLess(zone.upper, 105)
        for zone in result.resistances:
            self.assertGreater(zone.lower, 105)

    def test_old_observations_do_not_change_result(self):
        data = bars(80)
        cutoff = data.index.normalize().unique()[-60]
        expected = analyze_volume_profile(data.loc[data.index >= cutoff])
        data.loc[data.index < cutoff, "close"] = 100000
        data.loc[data.index < cutoff, "volume"] = 1000000
        actual = analyze_volume_profile(data)
        np.testing.assert_array_equal(actual.volume_shares, expected.volume_shares)
        np.testing.assert_array_equal(actual.kde_density, expected.kde_density)
        self.assertEqual(actual.supports, expected.supports)
        self.assertEqual(actual.start, expected.start)

    def test_no_forced_resistance_or_support(self):
        data = bars()
        data.iloc[-1, data.columns.get_loc("close")] = 150
        data.iloc[-1, data.columns.get_loc("volume")] = 0
        result = analyze_volume_profile(data)
        self.assertEqual(result.resistances, ())
        self.assertEqual(len(result.supports), 2)
        data.iloc[-1, data.columns.get_loc("close")] = 50
        result = analyze_volume_profile(data)
        self.assertEqual(result.supports, ())
        self.assertEqual(len(result.resistances), 2)

    def test_modes_at_observed_price_endpoints(self):
        index = pd.bdate_range("2026-01-01", periods=60, tz="Asia/Hong_Kong")
        data = pd.DataFrame({"close": np.tile([90.0, 110.0], 30),
                             "volume": np.full(60, 100.0)}, index=index)
        data.iloc[-1] = [100, 0]
        result = analyze_volume_profile(data)
        self.assertEqual(len(result.supports), 1)
        self.assertEqual(len(result.resistances), 1)
        support, resistance = result.supports[0], result.resistances[0]
        self.assertAlmostEqual(support.center, 90, delta=0.1)
        self.assertAlmostEqual(resistance.center, 110, delta=0.1)
        self.assertEqual(support.lower, 90)
        self.assertEqual(resistance.upper, 110)
        self.assertAlmostEqual(support.volume_share, 30 / 59)
        self.assertAlmostEqual(resistance.volume_share, 29 / 59)
        self.assertTrue(support.stable and resistance.stable)
        for zone in (support, resistance):
            self.assertLess(zone.lower, zone.upper)
            self.assertLessEqual(zone.lower, zone.center)
            self.assertLessEqual(zone.center, zone.upper)

    def test_current_price_inside_zone_is_omitted(self):
        data = bars()
        data.iloc[-1, 0] = 100
        result = analyze_volume_profile(data)
        self.assertEqual(len(result.supports), 1)
        self.assertTrue(all(z.lower > 100 for z in result.resistances))

    def test_volume_weighting_and_scale_invariance(self):
        data = bars()
        data.loc[data.close < 95, "volume"] = 5
        result = analyze_volume_profile(data)
        self.assertAlmostEqual(result.poc, 90, delta=1)
        data.volume *= 1000000
        scaled = analyze_volume_profile(data)
        np.testing.assert_allclose(result.volume_shares, scaled.volume_shares, atol=1e-14)
        np.testing.assert_allclose(result.kde_density, scaled.kde_density, atol=1e-14)

    def test_hk_session_dates_used_for_utc_input(self):
        data = bars()
        expected = analyze_volume_profile(data)
        data.index = data.index.tz_convert("UTC")
        actual = analyze_volume_profile(data)
        self.assertEqual(actual.start, expected.start)
        np.testing.assert_array_equal(actual.kde_density, expected.kde_density)

    def test_bad_parameters(self):
        data = bars()
        for kwargs in [{"lookback_sessions": 19}, {"lookback_sessions": 253},
                       {"lookback_sessions": True}, {"bins": 1}, {"bins": 257},
                       {"bandwidth": 0}, {"bandwidth": np.nan}, {"bandwidth": None},
                       {"bandwidth": 1j}, {"prominence": 1j},
                       {"prominence": 0}, {"prominence": 2}]:
            with self.subTest(kwargs=kwargs), self.assertRaises(VolumeProfileError):
                analyze_volume_profile(data, **kwargs)

    def test_invalid_data(self):
        for field, value in [("close", 0), ("close", np.inf), ("volume", -1),
                             ("volume", np.nan)]:
            data = bars()
            data.loc[data.index[0], field] = value
            with self.subTest(field=field, value=value), self.assertRaises(VolumeProfileError):
                analyze_volume_profile(data)
        for data in [bars(59), bars().iloc[::-1], bars().drop(columns="volume"),
                     pd.concat([bars(), bars().iloc[-1:]])]:
            with self.assertRaises(VolumeProfileError):
                analyze_volume_profile(data)
        data = bars()
        data.index = data.index.tz_localize(None)
        with self.assertRaises(VolumeProfileError):
            analyze_volume_profile(data)

    def test_nearby_windows_preserve_clear_synthetic_nodes(self):
        data = bars(80)
        shorter = analyze_volume_profile(data, lookback_sessions=50)
        longer = analyze_volume_profile(data, lookback_sessions=70)
        for short_zone, long_zone in zip(shorter.supports + shorter.resistances,
                                         longer.supports + longer.resistances):
            self.assertAlmostEqual(short_zone.center, long_zone.center, delta=0.5)

    def test_boolean_data_rejected(self):
        for field in ["close", "volume"]:
            data = bars()
            data[field] = True
            with self.assertRaises(VolumeProfileError):
                analyze_volume_profile(data)

    def test_zero_volume_flat_and_concentrated_prices(self):
        for kind in ["zero", "flat", "concentrated"]:
            data = bars()
            if kind == "zero":
                data.volume = 0
            elif kind == "flat":
                data.close = 100
            else:
                data.volume = 0
                data.iloc[-1, 1] = 1
            with self.subTest(kind=kind), self.assertRaises(VolumeProfileError):
                analyze_volume_profile(data)

    def test_value_area_brackets_poc_and_holds_share(self):
        result = analyze_volume_profile(bars(), value_area=0.7)
        self.assertIsNotNone(result.value_area_low)
        self.assertIsNotNone(result.value_area_high)
        self.assertLessEqual(result.value_area_low, result.poc)
        self.assertLessEqual(result.poc, result.value_area_high)
        edges = result.bin_edges
        histogram = result.volume_shares
        inside = 0.0
        for share, lower, upper in zip(histogram, edges[:-1], edges[1:]):
            if lower >= result.value_area_low - 1e-12 and upper <= result.value_area_high + 1e-12:
                inside += share
        self.assertGreaterEqual(inside, 0.7 - 1e-9)
        self.assertLess(inside, 0.7 + histogram.max())
        self.assertIsNone(result.decay_halflife)

    def test_decay_shifts_poc_toward_recent_volume(self):
        sessions = 60
        index = pd.bdate_range("2026-01-01", periods=sessions, tz="Asia/Hong_Kong")
        prices = np.r_[np.full(40, 90.0), np.full(20, 130.0)]
        data = pd.DataFrame({"close": prices, "volume": np.full(sessions, 100.0)}, index=index)
        equal = analyze_volume_profile(data)
        decayed = analyze_volume_profile(data, decay_halflife=10)
        self.assertLess(equal.poc, 100)
        self.assertGreater(decayed.poc, 110)
        self.assertEqual(decayed.decay_halflife, 10)
        # Without decay the two nodes have equal mass; with decay the old node fades.
        self.assertGreater(decayed.poc, equal.poc)

    def test_decay_parameters_validated(self):
        data = bars()
        for kwargs in [{"decay_halflife": 1}, {"decay_halflife": 253}, {"decay_halflife": 0},
                       {"decay_halflife": True}, {"decay_halflife": np.nan},
                       {"value_area": 0.5}, {"value_area": 0}, {"value_area": 1.2},
                       {"value_area": True}]:
            with self.subTest(kwargs=kwargs), self.assertRaises(VolumeProfileError):
                analyze_volume_profile(data, **kwargs)


if __name__ == "__main__":
    unittest.main()
