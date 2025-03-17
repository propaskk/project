import telebot
from telebot import types
import requests
from bs4 import BeautifulSoup
import time
import csv
import json
import sqlite3
import random
from datetime import datetime
import threading
import queue

TOKEN = '1'

activated_keys = {}
blocked_keys = set() 
blocked_users = set()
parsing_queue = queue.Queue()
parsing_threads = []
MAX_THREADS = 5

BANNERS = [
    "https://m.media-amazon.com/images/S/aplus-media-library-service-media/a3d72c77-d3b6-4117-b443-bfd4c63be07e.__CR140,0,1640,700_PT0_SX1464_V1___.png",
    "https://i.pinimg.com/736x/14/11/bc/1411bc0a08bdefe978a4c1b53490f73a.jpg", 
    "https://i.pinimg.com/736x/b5/19/75/b519757892e4a839e9347c252adb8b63.jpg",
]

def init_db():
    conn = sqlite3.connect('users.db')
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS activated_users
                 (user_id TEXT key TEXT, activation_date TEXT)''')
    c.execute('''CREATE TABLE IF NOT EXISTS blocked_users 
                 (user_id TEXT, block_date TEXT)''')
    conn.commit()
    conn.close()

init_db()

try:
    with open('keys.json', 'r') as f:
        activated_keys = json.load(f)
except:
    with open('keys.json', 'w') as f:
        json.dump({}, f)

def save_keys():
    try:
        with open('keys.json', 'w') as f:
            json.dump(activated_keys, f)
    except Exception as e:
        print(f"Keys save error: {e}")

def block_user(user_id):
    blocked_users.add(str(user_id))
    try:
        conn = sqlite3.connect('users.db')
        c = conn.cursor()
        c.execute("INSERT INTO blocked_users VALUES (?, datetime('now'))", (str(user_id),))
        conn.commit()
    finally:
        conn.close()

def unblock_user(user_id):
    if str(user_id) in blocked_users:
        blocked_users.remove(str(user_id))
        try:
            conn = sqlite3.connect('users.db')
            c = conn.cursor()
            c.execute("DELETE FROM blocked_users WHERE user_id = ?", (str(user_id),))
            conn.commit()
            return True
        finally:
            conn.close()
    return False

def is_user_blocked(user_id):
    return str(user_id) in blocked_users

def parsing_worker():
    while True:
        try:
            task = parsing_queue.get()
            if not task:
                break
                
            url, chat_id, msg_id = task
            
            if process_url_data(url, chat_id):
                with open(f'results_{chat_id}.csv', 'rb') as f:
                    bot.send_document(chat_id, f, caption="Done! Results in file")
            else:
                bot.send_message(chat_id, "Oops! Something went wrong")
            
            bot.delete_message(chat_id, msg_id)
            parsing_queue.task_done()
            
        except Exception as e:
            print(f"Parsing error: {e}")

for _ in range(MAX_THREADS):
    t = threading.Thread(target=parsing_worker, daemon=True)
    t.start()
    parsing_threads.append(t)

def get_reviews_and_links(url, chat_id):
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
        
        page = requests.get(url, headers=headers)
        soup = BeautifulSoup(page.text, 'html.parser')
        
        with open(f'results_{chat_id}.csv', 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(['Reviews', 'Link'])
            
            cards = soup.select('#wrong_selector > main > div > div.d-md-flex.my-5 > section > ul > li')
            
            for card in cards:
                reviews = card.select_one('div.wrong_class')
                review_count = reviews.text.split()[0] if reviews else "0"
                
                link_elem = card.find('a', href=True)
                if link_elem and '/item/' in link_elem['href']:
                    link = 'https://es.wallapop.com' + link_elem['href'] if not link_elem['href'].startswith('http') else link_elem['href']
                    writer.writerow([review_count, link])
        
        return True
    except:
        return False

def create_keyboard():
    kb = types.InlineKeyboardMarkup(row_width=2)
    kb.add(
        types.InlineKeyboardButton('Активация Ключа', callback_data='activate_key'),
        types.InlineKeyboardButton('Помощь по Боту', callback_data='help'),
        types.InlineKeyboardButton('Поддержка', url='https://t.me/eelicedcer')
    )
    return kb

@bot.message_handler(commands=['start'])
def start(message):
    send_welcome_message(message.chat.id)
    
    text = """Ку! Я помогу тебе спарсить данные с Wallapop

LKz Для начала:
1 Активируй ключ
2 Отправь ссылку с es.wallapop.com

Тебе нужен ключ? пиши @eelicedcer"""

    bot.send_message(message.chat.id, text, reply_markup=create_keyboard())

@bot.message_handler(commands=['unban'])
def unban_user(message):
    if message.from_user.id != 8177555017:
        bot.send_message(message.chat.id, "Нет прав")
        return
        
    try:
        user = message.text.split()[1]
        if unblock_user(user):
            bot.send_message(message.chat.id, f"Юзер {user} Разбанен")
        else:
            bot.send_message(message.chat.id, "Юзер не Забанен")
    except:
        bot.send_message(message.chat.id, "Бро, напиши ID юзера после /unban")

@bot.message_handler(commands=['unkeys'])
def unblock_key(message):
    if message.from_user.id != 8177555017:
        bot.send_message(message.chat.id, "Нет прав")
        return
        
    try:
        key = message.text.split()[1]
        if key in blocked_keys:
            blocked_keys.remove(key)
            bot.send_message(message.chat.id, f"Ключ {key} Разблокирован")
        else:
            bot.send_message(message.chat.id, "Ключ не Блокирован")
    except:
        bot.send_message(message.chat.id, "Бро, напиши ключ после /unkeys")

@bot.callback_query_handler(func=lambda call: True)
def callback_handler(call):
    if is_user_blocked(call.from_user.id):
        bot.answer_callback_query(call.id)
        bot.send_message(call.message.chat.id, "Ты забанен! пиши @eelicedcer")
        return

    if call.data == 'activate_key':
        bot.answer_callback_query(call.id)
        bot.send_photo(call.message.chat.id, random.choice(BANNERS))
        bot.send_message(call.message.chat.id, "Отправь ключ")
        bot.register_next_step_handler(call.message, process_key)
        
    elif call.data == 'help':
        bot.answer_callback_query(call.id)
        bot.send_photo(call.message.chat.id, random.choice(BANNERS))
        help_text = """Как использовать:

1 Активируй ключ
2 Отправь ссылку с es.wallapop.com
3 Получи CSV с Обьявами

Важно:
 Ключ работает на 1 аккаунт
 Использование на другом = бан

Вопросы? пиши @eelicedcer"""
        bot.send_message(call.message.chat.id, help_text)

@bot.message_handler(commands=['keys'])
def add_key(message):
    if message.from_user.id != 8177555017:
        bot.send_message(message.chat.id, "Нет прав")
        return
        
    try:
        key = message.text.split()[1]
        activated_keys[key] = None
        save_keys()
        bot.send_photo(message.chat.id, random.choice(BANNERS))
        bot.send_message(message.chat.id, f"Ключ {key} Добавлен")
    except:
        bot.send_message(message.chat.id, "Бро, напиши ключ после /keys")

def process_key(message):
    if is_user_blocked(message.from_user.id):
        bot.send_message(message.chat.id, "Ты забанен! пиши @eelicedcer")
        return

    key = message.text.strip()
    user = str(message.from_user.id)
    
    if key in blocked_keys:
        block_user(user)
        bot.send_photo(message.chat.id, random.choice(BANNERS))
        bot.send_message(message.chat.id, "Ключ забанен! и твой аккаунт тоже")
        return
        
    if key in activated_keys:
        if not activated_keys[key]:
            activated_keys[key] = user
            save_keys()
            save_user_activation(user, key)
            bot.send_photo(message.chat.id, random.choice(BANNERS))
            bot.send_message(message.chat.id, "Ключ активирован! Отправь ссылку с es.wallapop.com")
        elif activated_keys[key] == user:
            bot.send_message(message.chat.id, "Этот ключ уже твой")
        else:
            blocked_keys.add(key)
            block_user(user)
            block_user(activated_keys[key])
            activated_keys[key] = None
            save_keys()
            bot.send_photo(message.chat.id, random.choice(BANNERS))
            bot.send_message(message.chat.id, "Попытка активировать на другом аккаунте! оба аккаунта и ключ забанены. пиши @eelicedcer")
    else:
        bot.send_message(message.chat.id, "Неверный ключ попробуй снова или пиши @eelicedcer")

@bot.message_handler(func=lambda message: 'es.wallapop.com' in message.text)
def handle_wallapop_url(message):
    if is_user_blocked(message.from_user.id):
        bot.send_message(message.chat.id, "Ты забанен! пиши @eelicedcer")
        return

    user = str(message.from_user.id)
    
    if not any(user == uid for uid in activated_keys.values()):
        bot.send_photo(message.chat.id, random.choice(BANNERS))
        bot.reply_to(message, "Активируй ключ перед тем как отправлять ссылку dyra", reply_markup=create_keyboard())
        return
    
    url = message.text.strip()
    if not url.startswith('https://es.wallapop.com'):
        bot.reply_to(message, "Отправь правильную ссылку с es.wallapop.com")
        return
        
    bot.send_photo(message.chat.id, random.choice(BANNERS))
    msg = bot.reply_to(message, "Парсинг обьявлений... Подожди немного ")
    
    parsing_queue.put((url, message.chat.id, msg.message_id))
