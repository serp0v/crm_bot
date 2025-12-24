from telegram_notifier import TelegramNotifier

# Создаём экземпляр без вызова __init__, чтобы не требовать Telegram токен
tn = object.__new__(TelegramNotifier)

# Примерные данные: час -> count (локальные часы)
counts = {h: (h % 5 + 1) for h in range(24)}
# переставим шкалу так же, как в реальном использовании
start_hour = 8
hours = [(start_hour + i) % 24 for i in range(24)]
values = [counts.get(h, 0) for h in hours]

buf = tn._render_stats_image(values, hours, tz_name='Тест')
with open('test_stats.png', 'wb') as f:
    f.write(buf.getvalue())

print('Генерация изображения завершена, записан test_stats.png, размер:', len(buf.getvalue()))
