import numpy as np
import pytest

from ml.features import FEATURES, WINDOW, RollingFeatures


def test_feature_vector_shape_and_names():
    vec = RollingFeatures().update("s1", 20.0, 40.0, 0.5)
    assert len(vec) == len(FEATURES) == 9


def test_first_reading_has_zero_std_and_mean_equals_value():
    vec = RollingFeatures().update("s1", 20.0, 40.0, 0.5)
    assert vec[:3] == [20.0, 40.0, 0.5]
    assert vec[3:6] == [20.0, 40.0, 0.5]  # means
    assert vec[6:] == [0.0, 0.0, 0.0]  # stds


def test_rolling_mean_and_std_over_window():
    f = RollingFeatures(window=3)
    for t in (10.0, 20.0, 30.0):
        vec = f.update("s1", t, 0.0, 0.0)
    assert vec[3] == pytest.approx(20.0)
    assert vec[6] == pytest.approx(np.std([10.0, 20.0, 30.0]))


def test_old_readings_fall_out_of_window():
    f = RollingFeatures(window=2)
    for t in (100.0, 0.0, 10.0):
        vec = f.update("s1", t, 0.0, 0.0)
    assert vec[3] == pytest.approx(5.0)  # mean of [0, 10]; the 100 was dropped


def test_sensors_have_independent_windows():
    f = RollingFeatures()
    f.update("a", 100.0, 0.0, 0.0)
    vec = f.update("b", 1.0, 0.0, 0.0)
    assert vec[3] == 1.0


def test_default_window_constant():
    assert RollingFeatures().window == WINDOW
