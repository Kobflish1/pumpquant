"""Price a 0.1 SOL buy on a new Pump.fun token, then the same buy one trade later."""

from pumpquant.pricing.bonding_curve import pump_buy_exact_quote_in

SOL = 1_000_000_000  # lamports in one SOL; tokens have six decimals
FEES = {"protocol_fee_bps": 100, "creator_fee_bps": 0}  # a 1% fee
curve = {  # a new token's bonding curve
    "virtual_base_reserve": 1_073_000_000_000_000,
    "virtual_quote_reserve": 30 * SOL,
    "real_base_reserve": 793_100_000_000_000,
}

buy = pump_buy_exact_quote_in(gross_quote_amount=SOL // 10, **curve, **FEES)
print(f"0.1 SOL buys {buy.base_amount / 1e6:,.0f} tokens")

# Someone else's 1 SOL buy lands first and moves the curve.
other = pump_buy_exact_quote_in(gross_quote_amount=SOL, **curve, **FEES)
curve["virtual_quote_reserve"] += other.curve_quote_amount
curve["virtual_base_reserve"] -= other.base_amount
curve["real_base_reserve"] -= other.base_amount

late = pump_buy_exact_quote_in(gross_quote_amount=SOL // 10, **curve, **FEES)
fewer = 1 - late.base_amount / buy.base_amount
print(f"one trade later it buys {late.base_amount / 1e6:,.0f} tokens, {fewer:.1%} fewer")
