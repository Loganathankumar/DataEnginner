class SavingsAccount :

    def __init__(self, principal_amount, interest_rates, year) :

        self.principal_amount = principal_amount  # Initial amount invested
        self.interest_rates = interest_rates / 100  # Interest rate as a decimal
        self.year = year  # Investment period in years

    def calculate_interest(self) :
        # Calculate interest using simple interest formula
        interest = self.principal_amount * self.interest_rates * self.year
        return round(interest , 2)

    def calculate_savings_amount(self) :
        # Maturity amount is the sum of principal and interest earned
        maturity_amount = self.principal_amount + self.calculate_interest()
        return round(maturity_amount , 2)

    def display_account_summary(self) :
        interest = self.calculate_interest()
        maturity_amount = self.calculate_savings_amount()

        print(f"Principal Investment: {self.principal_amount} INR")
        print(f"Total Interest Earned: {interest} INR")
        print(f"Savings Amount after {self.year} years: {maturity_amount} INR")


# Example usage
principal = 600000  # 6 lakhs
interest_rate = float(input("Enter the interest rate: "))  # Input interest rate
years = 1  # 3 years tenure

savings_account = SavingsAccount(principal_amount=principal , interest_rates=interest_rate , year=years)
savings_account.display_account_summary()
