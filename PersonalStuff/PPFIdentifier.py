class PPFAccount:

    def __init__(self, monthly_deposit, interest_rate, years):
        self.monthly_deposit = monthly_deposit  # monthly deposit in INR
        self.interest_rate = interest_rate / 100  # convert percentage to decimal
        self.years = years  # investment period in years

    def calculate_maturity_amount(self):
        # Calculate annual deposit
        annual_deposit = self.monthly_deposit * 12  # convert monthly to yearly
        total_amount = 0

        for year in range(1, self.years + 1):
            # Compound annually
            total_amount = (total_amount + annual_deposit) * (1 + self.interest_rate)

        return round(total_amount, 2)

    def calculate_total_interest(self):
        total_investment = self.monthly_deposit * 12 * self.years
        maturity_amount = self.calculate_maturity_amount()
        total_interest = maturity_amount - total_investment
        return round(total_interest, 2)

    def display_account_summary(self):
        maturity_amount = self.calculate_maturity_amount()
        total_interest = self.calculate_total_interest()
        total_investment = self.monthly_deposit * 12 * self.years

        print(f"Total Investment: {total_investment} INR")
        print(f"Total Interest Earned: {total_interest} INR")
        print(f"Maturity Amount after {self.years} years: {maturity_amount} INR")

# Example usage
ppf = PPFAccount(monthly_deposit=5000, interest_rate=7.1, years=15)
ppf.display_account_summary()