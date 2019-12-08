from scipy.optimize import newton
import datetime

# EUROPEAN BK RECON & DEV 0% Oct2019

expected_ytm = 0.6  # Yield to Maturity

dt_today = datetime.datetime.now().date()
dt_end = datetime.datetime(2019, 10, 9).date()
coupon = 0.0
principal = 100.0  # It has to be equal to 100 when a bond price unit is percent like 101.25 (%)
bond_mkt_close_price = 86.731  # Close (USD)


def calculate_ytm(dt_begin, par_value, coupon_rate, dt_maturity, bond_close_price):
    # Calculations
    # Assuming Actual/365
    time_to_maturity = (dt_maturity - dt_begin).days / 365.0
    ytm_initial_guess = 0.0

    def objective_function(ytm):
        alpha = 1.0 / pow(1.0 + ytm, time_to_maturity)

        if ytm == 0:  # lim(x->0, (1-(1/(1+x)^n))/x) = n
            beta = time_to_maturity
        else:
            beta = (1 - alpha) / ytm

        bond_price = par_value * coupon_rate * beta + par_value * alpha

        return bond_price - bond_close_price  # To find a zero of the function

    return newton(objective_function, ytm_initial_guess)


ytm_solution = calculate_ytm(dt_today, principal, coupon, dt_end, bond_mkt_close_price)


print(f'expected: {expected_ytm}, real:{ytm_solution}')
