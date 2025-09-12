from app.tasks_analysis import _leg_effective_price, _dutch_book_ev, _bps_to_frac

def test_leg_effective_price_monotonic():
    p = 0.60
    small = _leg_effective_price(p, 100, taker_bps=20, age_sec=10)   # near 0.60 * (1+0.002)
    big   = _leg_effective_price(p, 1000, taker_bps=20, age_sec=10)  # slippage > small
    assert big >= small
    assert 0.60 < big < 0.70

def test_dutch_book_simple():
    # YES=0.65 on A, NO=0.40 on B at small size, tiny fees, no staleness
    legs = [
        {"effective_price": 0.65},
        {"effective_price": 0.40},
    ]
    ev, bps = _dutch_book_ev(legs, size_usd=100.0)
    # worst-case profit = min(100*(1-0.65)-100*0.40, 100*(1-0.40)-100*0.65) = min(-5, -5) = -5
    # That’s negative → not an arb unless prices are better (e.g., 0.55 + 0.40)
    assert ev <= 0
    # adjust to true Dutch: 0.55 + 0.40 = 0.95
    legs = [{"effective_price": 0.55}, {"effective_price": 0.40}]
    ev, bps = _dutch_book_ev(legs, 100.0)
    assert ev > 0
    assert bps > 0
