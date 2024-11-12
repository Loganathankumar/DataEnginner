import requests

url = "https://finance.yahoo.com/quote/AAPL/?p=AAPL"
print(url)

prop = "Previous Close"
response = requests.get(url)
print(response.status_code == 200)
t = response.text

ind = t.index("Previous Close")
redText = t[ind:].split("</span>")[1]
print(redText)
val = redText.split(">")[-2]
print(val)
