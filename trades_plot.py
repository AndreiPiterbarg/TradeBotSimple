import matplotlib.pyplot as plt
import json

trade_list = []
with open('trades.json', 'r') as f:
    trades_data = json.load(f)

prices = [float(trade['price']) for trade in trades_data]

plt.plot(prices)
plt.show()