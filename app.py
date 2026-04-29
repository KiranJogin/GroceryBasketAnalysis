from flask import Flask, render_template, request, jsonify, session, redirect, url_for, flash, send_file
import pandas as pd
import numpy as np
import sqlite3
import plotly
import plotly.express as px
import plotly.graph_objects as go
import json
from datetime import datetime, timedelta
import random
import hashlib
from itertools import combinations
from collections import Counter
import io
import re
from werkzeug.utils import secure_filename
import os

app = Flask(__name__)
app.secret_key = 'your-super-secret-key-change-in-production'
app.config['UPLOAD_FOLDER'] = 'static/uploads'
app.config['MAX_CONTENT_LENGTH'] = 16 * 1024 * 1024

DB_NAME = "grocery_basket.db"

@app.context_processor
def inject_user():
    if 'user_id' not in session:
        return {'user': None}
    return {'user': session}

# =====================================================
# DATABASE HELPERS (SAME AS ORIGINAL)
# =====================================================
def get_conn():
    return sqlite3.connect(DB_NAME, check_same_thread=False)

def hash_password(password):
    return hashlib.sha256(password.encode()).hexdigest()

def query_df(query, params=()):
    conn = get_conn()
    df = pd.read_sql_query(query, conn, params=params)
    conn.close()
    return df

def sanitize_identifier(name):
    cleaned = re.sub(r"\W+", "_", str(name).strip().lower()).strip("_")
    return cleaned or "column"

def unique_columns(columns):
    seen = {}
    result = []
    for column in columns:
        base = sanitize_identifier(column)
        count = seen.get(base, 0)
        seen[base] = count + 1
        result.append(base if count == 0 else f"{base}_{count + 1}")
    return result

def get_table_columns(conn, table_name):
    return [row[1] for row in conn.execute(f"PRAGMA table_info({table_name})").fetchall()]

def find_matching_schema(conn, columns):
    managed_tables = [
        "products",
        "transactions",
        "transaction_items",
        "recommendations",
        "alerts",
        "coupons",
    ]
    optional_ids = {
        "transaction_items": "item_id",
        "recommendations": "rec_id",
        "alerts": "alert_id",
    }
    normalized_upload = [sanitize_identifier(col) for col in columns]

    for table in managed_tables:
        table_columns = get_table_columns(conn, table)
        normalized_table = [sanitize_identifier(col) for col in table_columns]
        if normalized_upload == normalized_table:
            return table, table_columns

        optional_id = optional_ids.get(table)
        if optional_id and optional_id in table_columns:
            without_id = [col for col in table_columns if col != optional_id]
            if normalized_upload == [sanitize_identifier(col) for col in without_id]:
                return table, without_id

    return None, None

def init_db():
    conn = get_conn()
    cur = conn.cursor()

    cur.execute('''CREATE TABLE IF NOT EXISTS users (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT,
        email TEXT UNIQUE,
        password TEXT,
        role TEXT,
        created_at TEXT
    )''')

    cur.execute('''CREATE TABLE IF NOT EXISTS products (
        product_id TEXT PRIMARY KEY,
        product_name TEXT,
        category TEXT,
        brand TEXT,
        price REAL,
        cost REAL,
        stock INTEGER,
        created_at TEXT
    )''')

    cur.execute('''CREATE TABLE IF NOT EXISTS transactions (
        transaction_id TEXT PRIMARY KEY,
        customer_id TEXT,
        basket_date TEXT,
        payment_method TEXT,
        total_amount REAL,
        discount_amount REAL,
        final_amount REAL,
        created_at TEXT
    )''')

    cur.execute("PRAGMA table_info(transactions)")
    transaction_columns = [col[1] for col in cur.fetchall()]
    if "coupon_code" not in transaction_columns:
        cur.execute("ALTER TABLE transactions ADD COLUMN coupon_code TEXT")

    cur.execute('''CREATE TABLE IF NOT EXISTS transaction_items (
        item_id INTEGER PRIMARY KEY AUTOINCREMENT,
        transaction_id TEXT,
        product_id TEXT,
        quantity INTEGER,
        unit_price REAL,
        line_total REAL
    )''')

    cur.execute('''CREATE TABLE IF NOT EXISTS recommendations (
        rec_id INTEGER PRIMARY KEY AUTOINCREMENT,
        base_product TEXT,
        recommended_product TEXT,
        support_score REAL,
        confidence_score REAL,
        generated_at TEXT
    )''')

    cur.execute('''CREATE TABLE IF NOT EXISTS alerts (
        alert_id INTEGER PRIMARY KEY AUTOINCREMENT,
        transaction_id TEXT,
        alert_type TEXT,
        severity TEXT,
        description TEXT,
        created_at TEXT
    )''')

    cur.execute('''CREATE TABLE IF NOT EXISTS coupons (
        coupon_code TEXT PRIMARY KEY,
        discount_percent REAL,
        min_purchase REAL,
        active INTEGER
    )''')

    cur.execute("SELECT * FROM users WHERE email=?", ("admin@grocery.com",))
    if not cur.fetchone():
        cur.execute(
            "INSERT INTO users (name,email,password,role,created_at) VALUES (?,?,?,?,?)",
            ("Retail Admin", "admin@grocery.com", hash_password("admin123"), "admin", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        )

    default_coupons = [
        ("SAVE5", 5, 500, 1),
        ("SAVE10", 10, 1200, 1),
        ("FEST15", 15, 2000, 1)
    ]
    for c in default_coupons:
        cur.execute("INSERT OR IGNORE INTO coupons VALUES (?,?,?,?)", c)

    conn.commit()
    conn.close()

def login_user(email, password):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        "SELECT id, name, email, role FROM users WHERE email=? AND password=?",
        (email, hash_password(password))
    )
    user = cur.fetchone()
    conn.close()
    return user

def register_user(name, email, password):
    try:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO users (name,email,password,role,created_at) VALUES (?,?,?,?,?)",
            (name, email, hash_password(password), "analyst", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        )
        conn.commit()
        conn.close()
        return True
    except:
        return False

# =====================================================
# COUPON HELPERS (SAME)
# =====================================================
def add_coupon(coupon_code, discount_percent, min_purchase, active=1):
    try:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO coupons (coupon_code, discount_percent, min_purchase, active) VALUES (?,?,?,?)",
            (coupon_code.upper(), discount_percent, min_purchase, active)
        )
        conn.commit()
        conn.close()
        return True
    except:
        return False

def update_coupon_status(coupon_code, active):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("UPDATE coupons SET active=? WHERE coupon_code=?", (active, coupon_code))
    conn.commit()
    conn.close()

def delete_coupon(coupon_code):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("DELETE FROM coupons WHERE coupon_code=?", (coupon_code,))
    conn.commit()
    conn.close()

def calculate_coupon(total, coupon_code):
    coupon_code = coupon_code.strip().upper()
    coupon_df = query_df("SELECT * FROM coupons WHERE coupon_code=? AND active=1", (coupon_code,))
    if coupon_df.empty:
        return 0, "Invalid / inactive coupon"

    row = coupon_df.iloc[0]
    if total < row['min_purchase']:
        return 0, f"Minimum purchase for {coupon_code} is ₹{row['min_purchase']}"

    discount = round(total * row['discount_percent'] / 100, 2)
    return discount, f"Coupon {coupon_code} applied successfully"

# =====================================================
# DATA GENERATION (SAME)
# =====================================================
def generate_demo_data(force=False):
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("SELECT COUNT(*) FROM products")
    if cur.fetchone()[0] > 0 and not force:
        conn.close()
        return

    if force:
        cur.executescript("""
            DELETE FROM transaction_items;
            DELETE FROM transactions;
            DELETE FROM recommendations;
            DELETE FROM alerts;
            DELETE FROM products;
        """)

    categories = {
        "Dairy": ["Milk", "Cheese", "Curd", "Butter", "Paneer"],
        "Bakery": ["Bread", "Bun", "Cake", "Cookies", "Muffin"],
        "Beverages": ["Tea", "Coffee", "Juice", "Soda", "Water Bottle"],
        "Snacks": ["Chips", "Nachos", "Biscuits", "Popcorn", "Nuts"],
        "Household": ["Detergent", "Dishwash", "Tissue", "Toilet Cleaner", "Garbage Bags"],
        "Personal Care": ["Soap", "Shampoo", "Toothpaste", "Face Wash", "Lotion"],
        "Fruits": ["Apple", "Banana", "Orange", "Mango", "Grapes"],
        "Vegetables": ["Tomato", "Potato", "Onion", "Carrot", "Capsicum"]
    }

    brands = ["FreshMart", "DailyNeeds", "UrbanBasket", "PrimeChoice", "ValueBuy"]
    products = []
    i = 1

    for cat, items in categories.items():
        for item in items:
            pid = f"PRD{i:03d}"
            price = round(random.uniform(20, 450), 2)
            cost = round(price * random.uniform(0.55, 0.82), 2)
            stock = random.randint(40, 300)
            brand = random.choice(brands)
            cur.execute(
                "INSERT OR IGNORE INTO products VALUES (?,?,?,?,?,?,?,?)",
                (pid, item, cat, brand, price, cost, stock, datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
            )
            products.append((pid, item, cat, price))
            i += 1

    for t in range(1, 351):
        txn_id = f"TXN{datetime.now().strftime('%Y%m')}{t:04d}"
        cust_id = f"CUS{random.randint(1,150):04d}"
        txn_date = (datetime.now() - timedelta(days=random.randint(0, 150))).strftime("%Y-%m-%d")
        payment_method = random.choice(["UPI", "Card", "Cash", "Wallet"])
        basket_size = random.randint(2, 8)
        chosen = random.sample(products, basket_size)

        names = [x[1] for x in chosen]
        if "Bread" in names and random.random() < 0.7:
            chosen.append([x for x in products if x[1] == "Butter"][0])
        if "Tea" in names and random.random() < 0.6:
            chosen.append([x for x in products if x[1] == "Biscuits"][0])
        if "Milk" in names and random.random() < 0.55:
            chosen.append([x for x in products if x[1] == "Cookies"][0])

        chosen = list({x[0]: x for x in chosen}.values())

        total = 0
        for pr in chosen:
            qty = random.randint(1, 4)
            line_total = round(pr[3] * qty, 2)
            total += line_total
            cur.execute(
                "INSERT INTO transaction_items (transaction_id,product_id,quantity,unit_price,line_total) VALUES (?,?,?,?,?)",
                (txn_id, pr[0], qty, pr[3], line_total)
            )

        discount = 0
        if total > 2000:
            discount = round(total * 0.08, 2)

        final_amount = round(total - discount, 2)

        cur.execute(
            """INSERT OR IGNORE INTO transactions
               (transaction_id,customer_id,basket_date,payment_method,total_amount,discount_amount,final_amount,created_at)
               VALUES (?,?,?,?,?,?,?,?)""",
            (txn_id, cust_id, txn_date, payment_method, round(total,2), discount, final_amount, datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        )

        if final_amount > 2500:
            cur.execute(
                "INSERT INTO alerts (transaction_id,alert_type,severity,description,created_at) VALUES (?,?,?,?,?)",
                (txn_id, "High Basket Value", "Medium", "Unusually high basket amount detected", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
            )

    conn.commit()
    conn.close()
    generate_recommendations()

def generate_recommendations():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("DELETE FROM recommendations")

    items = query_df("""
        SELECT ti.transaction_id, p.product_name
        FROM transaction_items ti
        JOIN products p ON ti.product_id = p.product_id
    """)

    if items.empty:
        conn.close()
        return

    baskets = items.groupby('transaction_id')['product_name'].apply(list).tolist()
    pair_counts = Counter()
    item_counts = Counter()
    total_baskets = len(baskets)

    for basket in baskets:
        unique = list(set(basket))
        for item in unique:
            item_counts[item] += 1
        for pair in combinations(sorted(unique), 2):
            pair_counts[pair] += 1

    for (a, b), count in pair_counts.items():
        support = round(count / total_baskets, 4)
        conf_ab = round(count / item_counts[a], 4)
        conf_ba = round(count / item_counts[b], 4)

        cur.execute(
            "INSERT INTO recommendations (base_product,recommended_product,support_score,confidence_score,generated_at) VALUES (?,?,?,?,?)",
            (a, b, support, conf_ab, datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        )
        cur.execute(
            "INSERT INTO recommendations (base_product,recommended_product,support_score,confidence_score,generated_at) VALUES (?,?,?,?,?)",
            (b, a, support, conf_ba, datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        )

    conn.commit()
    conn.close()

# =====================================================
# BUSINESS LOGIC (SAME)
# =====================================================
def add_transaction(customer_id, basket_date, payment_method, selected_products, coupon_code=""):
    if not selected_products or len(selected_products) < 2:
        raise ValueError("Please select at least 2 products.")

    conn = get_conn()
    cur = conn.cursor()

    txn_id = f"TXN{datetime.now().strftime('%Y%m%d%H%M%S')}{random.randint(100,999)}"
    total = 0
    item_rows = []

    for product_id, qty in selected_products:
        qty = int(qty)
        if qty < 1:
            continue
        prod = query_df("SELECT * FROM products WHERE product_id=?", (product_id,))
        if prod.empty:
            continue
        price = float(prod.iloc[0]['price'])
        line_total = round(price * qty, 2)
        total += line_total
        item_rows.append((txn_id, product_id, qty, price, line_total))

    if len(item_rows) < 2:
        conn.close()
        raise ValueError("Please select at least 2 valid products.")

    discount = 0
    msg = "No coupon applied"
    applied_coupon = None

    if coupon_code.strip():
        normalized_coupon = coupon_code.strip().upper()
        discount, msg = calculate_coupon(total, normalized_coupon)
        if discount > 0:
            applied_coupon = normalized_coupon

    final_amount = round(total - discount, 2)

    for row in item_rows:
        cur.execute(
            "INSERT INTO transaction_items (transaction_id,product_id,quantity,unit_price,line_total) VALUES (?,?,?,?,?)",
            row
        )

    cur.execute(
        """INSERT INTO transactions
           (transaction_id,customer_id,basket_date,payment_method,total_amount,discount_amount,final_amount,created_at,coupon_code)
           VALUES (?,?,?,?,?,?,?,?,?)""",
        (txn_id, customer_id, basket_date, payment_method, round(total,2), discount, final_amount, datetime.now().strftime("%Y-%m-%d %H:%M:%S"), applied_coupon)
    )

    conn.commit()
    conn.close()
    generate_recommendations()
    return txn_id, msg

# =====================================================
# LOAD GLOBAL DATA
# =====================================================
def load_data():
    products_df = query_df("SELECT * FROM products")
    transactions_df = query_df("SELECT * FROM transactions")
    items_df = query_df("SELECT * FROM transaction_items")
    recommend_df = query_df("SELECT * FROM recommendations")
    alerts_df = query_df("SELECT * FROM alerts")
    coupons_df = query_df("SELECT * FROM coupons")

    basket_df = items_df.merge(
        products_df[['product_id','product_name','category','brand','cost']],
        on='product_id',
        how='left'
    ).merge(
        transactions_df[['transaction_id','basket_date','payment_method','total_amount','discount_amount','final_amount','customer_id']],
        on='transaction_id',
        how='left'
    )
    return {
        'products': products_df.to_dict('records'),
        'transactions': transactions_df.to_dict('records'),
        'items': items_df.to_dict('records'),
        'recommendations': recommend_df.to_dict('records'),
        'alerts': alerts_df.to_dict('records'),
        'coupons': coupons_df.to_dict('records'),
        'basket': basket_df.to_dict('records')
    }

# =====================================================
# PLOTLY CHART GENERATOR
# =====================================================
def fig_to_plotly_json(fig):
    return json.loads(fig.to_json())

# =====================================================
# ROUTES
# =====================================================
@app.route('/')
def index():
    if 'user_id' not in session:
        return redirect(url_for('login'))
    return redirect(url_for('dashboard'))

@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        email = request.form['email']
        password = request.form['password']
        user = login_user(email, password)
        if user:
            session['user_id'] = user[0]
            session['user_name'] = user[1]
            session['user_email'] = user[2]
            session['user_role'] = user[3]
            flash('Login successful!', 'success')
            return redirect(url_for('dashboard'))
        flash('Invalid credentials', 'error')
    return render_template('login.html')

@app.route('/register', methods=['GET', 'POST'])
def register():
    if request.method == 'POST':
        name = request.form['name']
        email = request.form['email']
        password = request.form['password']
        if register_user(name, email, password):
            flash('Registration successful! Please login.', 'success')
            return redirect(url_for('login'))
        flash('Registration failed. Email may already exist.', 'error')
    return render_template('register.html')

@app.route('/logout')
def logout():
    session.clear()
    flash('Logged out successfully', 'info')
    return redirect(url_for('login'))

@app.route('/dashboard')
def dashboard():
    if 'user_id' not in session:
        return redirect(url_for('login'))
    
    data = load_data()
    transactions_df = pd.DataFrame(data['transactions'])
    
    total_sales = transactions_df['final_amount'].sum()
    total_txn = len(transactions_df)
    avg_basket = transactions_df['final_amount'].mean()
    gross_sales = transactions_df['total_amount'].sum()

    profit_df = pd.DataFrame(data['basket'])
    profit_df['profit'] = profit_df['line_total'] - (profit_df['cost'] * profit_df['quantity'])
    total_profit = profit_df['profit'].sum()

    # Charts
    daily = transactions_df.groupby('basket_date', as_index=False)[['final_amount','discount_amount']].sum().sort_values('basket_date')
    fig1 = px.line(daily, x='basket_date', y='final_amount', title='Net Sales Trend')

    top_prod = profit_df.groupby('product_name', as_index=False)['quantity'].sum().sort_values('quantity', ascending=False).head(12)
    fig2 = px.bar(top_prod, x='product_name', y='quantity', text='quantity', title='Top Selling Products')

    cat = profit_df.groupby('category', as_index=False)['line_total'].sum()
    fig3 = px.pie(cat, names='category', values='line_total', hole=0.45, title='Category Revenue Share')

    metrics = {
        'total_txn': int(total_txn),
        'total_sales': f"₹ {total_sales:,.0f}",
        'gross_sales': f"₹ {gross_sales:,.0f}",
        'avg_basket': f"₹ {avg_basket:,.0f}",
        'total_profit': f"₹ {total_profit:,.0f}"
    }

    charts = {
        'sales_trend': fig_to_plotly_json(fig1),
        'top_products': fig_to_plotly_json(fig2),
        'category_pie': fig_to_plotly_json(fig3)
    }

    recent_txns = transactions_df.sort_values('basket_date', ascending=False).head(10).to_dict('records')

    return render_template('dashboard.html', 
                         user=session, 
                         metrics=metrics, 
                         charts=charts,
                         recent_txns=recent_txns)

@app.route('/api/checkout/validate_coupon', methods=['POST'])
def validate_coupon():
    data = request.json
    total = data['total']
    coupon_code = data['coupon_code']
    discount, message = calculate_coupon(total, coupon_code)
    return jsonify({'discount': discount, 'message': message})

@app.route('/receipt_center')
def receipt_center():
    if 'user_id' not in session:
        return redirect(url_for('login'))
    transactions_df = query_df("SELECT * FROM transactions ORDER BY basket_date DESC")
    return render_template('receipt_center.html', transactions=transactions_df.to_dict('records'))

@app.route('/api/checkout/create', methods=['POST'])
def create_transaction():
    if 'user_id' not in session:
        return jsonify({'error': 'Please login before creating a transaction.'}), 401

    data = request.json
    customer_id = data['customer_id']
    basket_date = data['basket_date']
    payment_method = data['payment_method']
    selected_products = data['selected_products']
    coupon_code = data.get('coupon_code', '')
    
    try:
        txn_id, message = add_transaction(customer_id, basket_date, payment_method, selected_products, coupon_code)
    except ValueError as exc:
        return jsonify({'error': str(exc)}), 400

    return jsonify({'txn_id': txn_id, 'message': message, 'receipt_url': url_for('receipt', txn_id=txn_id)})

@app.route('/checkout')
def checkout():
    if 'user_id' not in session:
        return redirect(url_for('login'))
    products = query_df("SELECT product_id, product_name, category, price FROM products").to_dict('records')
    coupons = query_df("SELECT * FROM coupons WHERE active=1").to_dict('records')
    current_date = datetime.now().strftime("%Y-%m-%d")
    return render_template('checkout.html', products=products, coupons=coupons, current_date=current_date, user=session)

@app.route('/receipt/<txn_id>')
def receipt(txn_id):
    if 'user_id' not in session:
        return redirect(url_for('login'))

    init_db()
    
    txn = query_df("SELECT * FROM transactions WHERE transaction_id=?", (txn_id,))
    if txn.empty:
        flash('Receipt not found', 'error')
        return redirect(url_for('dashboard'))
    
    items = query_df("""
        SELECT ti.*, p.product_name, p.category
        FROM transaction_items ti
        JOIN products p ON ti.product_id = p.product_id
        WHERE ti.transaction_id=?
    """, (txn_id,))
    
    return render_template('receipt.html', txn=txn.iloc[0], items=items.to_dict('records'), user=session)

@app.route('/api/download_receipt/<txn_id>')
def download_receipt(txn_id):
    init_db()
    # Comprehensive receipt with all details
    txn = query_df("""
        SELECT 
            'FreshMart Receipt' as store_name,
            t.transaction_id,
            t.customer_id,
            t.basket_date,
            t.payment_method,
            t.total_amount as gross_amount,
            COALESCE(t.discount_amount, 0) as discount_amount,
            COALESCE(t.coupon_code, '') as coupon_code,
            t.final_amount as net_amount,
            t.created_at as receipt_date,
            ti.quantity,
            p.product_name,
            p.category,
            p.brand,
            ti.unit_price,
            ti.line_total,
            ROW_NUMBER() OVER (PARTITION BY t.transaction_id ORDER BY ti.item_id) as line_number
        FROM transactions t
        LEFT JOIN transaction_items ti ON t.transaction_id = ti.transaction_id
        LEFT JOIN products p ON ti.product_id = p.product_id
        WHERE t.transaction_id = ?
        ORDER BY ti.item_id
    """, (txn_id,))
    
    if txn.empty:
        return 'Receipt not found', 404
    
    return send_file(
        io.BytesIO(txn.to_csv(index=False, encoding='utf-8-sig').encode('utf-8')),
        as_attachment=True,
        download_name=f"FreshMart_Receipt_{txn_id}.csv",
        mimetype='text/csv'
    )

@app.route('/market_basket')
def market_basket():
    if 'user_id' not in session:
        return redirect(url_for('login'))
    
    recommend_df = query_df("SELECT * FROM recommendations")
    top_pairs = recommend_df.groupby(
        ['base_product','recommended_product'],
        as_index=False
    )[['support_score','confidence_score']].mean().sort_values(
        ['confidence_score','support_score'],
        ascending=False
    ).head(25)
    
    fig = px.scatter(
        top_pairs,
        x='support_score',
        y='confidence_score',
        size='confidence_score',
        hover_name='base_product',
        color='base_product'
    )
    
    return render_template('market_basket.html', 
                         top_pairs=top_pairs.to_dict('records'),
                         chart_data=json.dumps(top_pairs.to_dict('records')),
                         user=session)

@app.route('/recommendations')
def recommendations():
    if 'user_id' not in session:
        return redirect(url_for('login'))
    return render_template('recommendations.html', user=session)

@app.route('/api/recommendations/<base_product>')
def get_recommendations(base_product):
    recs = query_df("""
        SELECT * FROM recommendations 
        WHERE base_product=? 
        ORDER BY confidence_score DESC 
        LIMIT 10
    """, (base_product,))
    return jsonify(recs.to_dict('records'))

@app.route('/profit')
def profit():
    if 'user_id' not in session:
        return redirect(url_for('login'))
    
    data = load_data()
    profit_df = pd.DataFrame(data['basket'])
    profit_df['profit'] = profit_df['line_total'] - (profit_df['cost'] * profit_df['quantity'])
    
    # Top 20 profitable products
    prod_profit = profit_df.groupby('product_name', as_index=False)['profit'].sum().sort_values('profit', ascending=False).head(20)
    
    # Top 20 by profit rate (margin %)
    profit_df['profit_rate'] = profit_df['profit'] / profit_df['line_total'] * 100
    prod_margin = profit_df.groupby('product_name', as_index=False)['profit_rate'].mean().sort_values('profit_rate', ascending=False).head(20)
    
    low_stock = query_df("SELECT * FROM products WHERE stock < 60")
    coupons = query_df("SELECT * FROM coupons")
    
    return render_template('profit.html',
                         prod_profit=prod_profit.to_dict('records'),
                         prod_margin=prod_margin.to_dict('records'),
                         low_stock=low_stock.to_dict('records'),
                         coupons=coupons.to_dict('records'),
                         chart_data1=json.dumps(prod_profit.to_dict('records')),
                         chart_data2=json.dumps(prod_margin.to_dict('records')),
                         user=session)

@app.route('/customers')
def customers():
    if 'user_id' not in session:
        return redirect(url_for('login'))
    
    data = load_data()
    basket_df = pd.DataFrame(data['basket'])
    
    cust = basket_df.groupby('customer_id', as_index=False).agg(
        transactions=('transaction_id','nunique'),
        total_spend=('line_total','sum'),
        avg_basket_value=('final_amount','mean'),
        unique_products=('product_name','nunique')
    ).sort_values('total_spend', ascending=False)
    
    fig1 = px.scatter(cust, x='transactions', y='total_spend', size='unique_products', hover_name='customer_id')
    fig2 = px.histogram(cust, x='avg_basket_value', nbins=20)
    
    return render_template('customers.html',
                         customers=cust.to_dict('records'),
                         user=session)

@app.route('/reports')
def reports():
    if 'user_id' not in session:
        return redirect(url_for('login'))
    return render_template('reports.html', user=session)

@app.route('/api/reports/<report_type>')
def get_report(report_type):
    reports = {
        'products': "SELECT * FROM products",
        'transactions': "SELECT * FROM transactions",
        'items': "SELECT * FROM transaction_items",
        'recommendations': "SELECT * FROM recommendations",
        'alerts': "SELECT * FROM alerts",
        'coupons': "SELECT * FROM coupons",
        'basket': """
            SELECT
                ti.item_id,
                ti.transaction_id,
                ti.product_id,
                p.product_name,
                p.category,
                p.brand,
                ti.quantity,
                ti.unit_price,
                ti.line_total,
                p.price,
                p.cost,
                p.stock,
                t.customer_id,
                t.basket_date,
                t.payment_method,
                t.total_amount,
                t.discount_amount,
                t.final_amount,
                t.coupon_code,
                t.created_at AS transaction_created_at
            FROM transaction_items ti
            JOIN products p ON ti.product_id = p.product_id
            JOIN transactions t ON ti.transaction_id = t.transaction_id
        """,
        'basket_stats': "SELECT ROUND(AVG(quantity),1) as avg_basket_size FROM (SELECT transaction_id, SUM(quantity) as quantity FROM transaction_items GROUP BY transaction_id)"
    }
    
    if report_type in reports:
        df = query_df(reports[report_type])
        return jsonify(df.to_dict('records'))
    return jsonify({'error': 'Report not found'}), 404

@app.route('/dataset')
def dataset():
    if 'user_id' not in session:
        return redirect(url_for('login'))
    return render_template('dataset.html', user=session)

@app.route('/api/dataset/download_full')
def download_full_dataset():
    if 'user_id' not in session:
        return redirect(url_for('login'))

    df = query_df("""
        SELECT
            ti.item_id,
            ti.transaction_id,
            ti.product_id,
            p.product_name,
            p.category,
            p.brand,
            ti.quantity,
            ti.unit_price,
            ti.line_total,
            p.price,
            p.cost,
            p.stock,
            t.customer_id,
            t.basket_date,
            t.payment_method,
            t.total_amount,
            t.discount_amount,
            t.final_amount,
            t.coupon_code,
            t.created_at AS transaction_created_at
        FROM transaction_items ti
        JOIN products p ON ti.product_id = p.product_id
        JOIN transactions t ON ti.transaction_id = t.transaction_id
        ORDER BY t.basket_date DESC, ti.transaction_id, ti.item_id
    """)

    return send_file(
        io.BytesIO(df.to_csv(index=False).encode('utf-8-sig')),
        as_attachment=True,
        download_name='grocery_basket_dataset.csv',
        mimetype='text/csv'
    )

@app.route('/api/dataset/apply', methods=['POST'])
def apply_custom_dataset():
    try:
        if 'user_id' not in session:
            return jsonify({'error': 'Please login before uploading a dataset.'}), 401

        if 'dataset_file' not in request.files:
            return jsonify({'error': 'No dataset file was uploaded.'}), 400

        uploaded_file = request.files['dataset_file']
        if not uploaded_file.filename:
            return jsonify({'error': 'Choose a CSV file before applying.'}), 400

        filename = secure_filename(uploaded_file.filename)
        if not filename.lower().endswith('.csv'):
            return jsonify({'error': 'Only CSV uploads are supported right now.'}), 400

        file_bytes = uploaded_file.read()
        read_errors = []
        df = None
        for encoding in ("utf-8-sig", "utf-8", "cp1252", "latin1"):
            try:
                df = pd.read_csv(io.BytesIO(file_bytes), encoding=encoding)
                break
            except UnicodeDecodeError as exc:
                read_errors.append(f"{encoding}: {exc}")
            except Exception as exc:
                read_errors.append(f"{encoding}: {exc}")

        if df is None:
            return jsonify({
                'error': 'Could not read CSV file. Tried UTF-8, Windows-1252, and Latin-1 encodings.',
                'details': read_errors[-1] if read_errors else ''
            }), 400

        if df.empty or len(df.columns) == 0:
            return jsonify({'error': 'The uploaded CSV does not contain any rows or columns.'}), 400

        df.columns = unique_columns(df.columns)

        conn = get_conn()
        try:
            matched_table, matched_columns = find_matching_schema(conn, df.columns)
            if matched_table:
                df = df[matched_columns]
                conn.execute(f"DELETE FROM {matched_table}")
                df.to_sql(matched_table, conn, if_exists='append', index=False)
                conn.commit()
                conn.close()
                conn = None
                if matched_table in {"products", "transactions", "transaction_items"}:
                    generate_recommendations()
                return jsonify({
                    'mode': 'existing_schema',
                    'message': f'Applied {len(df)} rows to the existing {matched_table} table.',
                    'table': matched_table,
                    'rows': int(len(df)),
                    'columns': list(df.columns)
                })

            os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)
            dataset_name = os.path.splitext(filename)[0]
            timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
            db_filename = f"{sanitize_identifier(dataset_name)}_{timestamp}.db"
            db_path = os.path.join(app.config['UPLOAD_FOLDER'], db_filename)

            custom_conn = sqlite3.connect(db_path)
            try:
                df.to_sql('custom_dataset', custom_conn, if_exists='replace', index=False)
                custom_conn.execute(
                    "CREATE TABLE IF NOT EXISTS dataset_metadata (key TEXT PRIMARY KEY, value TEXT)"
                )
                metadata = {
                    'source_file': filename,
                    'created_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    'rows': str(len(df)),
                    'columns': ','.join(df.columns),
                }
                custom_conn.executemany(
                    "INSERT OR REPLACE INTO dataset_metadata (key,value) VALUES (?,?)",
                    metadata.items()
                )
                custom_conn.commit()
            finally:
                custom_conn.close()

            return jsonify({
                'mode': 'new_database',
                'message': 'Columns did not match the grocery schema, so a separate SQLite database was created.',
                'database': db_path,
                'table': 'custom_dataset',
                'rows': int(len(df)),
                'columns': list(df.columns)
            })
        finally:
            if conn:
                conn.close()
    except Exception as exc:
        app.logger.exception("Dataset upload failed")
        return jsonify({'error': f'Dataset upload failed: {exc}'}), 500

@app.route('/coupons')
def coupons():
    if 'user_id' not in session:
        return redirect(url_for('login'))
    coupons_df = query_df("SELECT * FROM coupons")
    return render_template('coupons.html', coupons=coupons_df.to_dict('records'), user=session)

@app.route('/api/coupons', methods=['POST'])
def api_coupons():
    action = request.json.get('action')
    
    if action == 'create':
        coupon_code = request.json['coupon_code']
        discount = request.json['discount']
        min_purchase = request.json['min_purchase']
        active = 1 if request.json['active'] == 'Active' else 0
        if add_coupon(coupon_code, discount, min_purchase, active):
            return jsonify({'success': True})
        return jsonify({'success': False}), 400
    
    elif action == 'update_status':
        coupon_code = request.json['coupon_code']
        active = request.json['active']
        update_coupon_status(coupon_code, active)
        return jsonify({'success': True})
    
    elif action == 'delete':
        coupon_code = request.json['coupon_code']
        delete_coupon(coupon_code)
        return jsonify({'success': True})
    
    return jsonify({'error': 'Invalid action'}), 400

@app.route('/admin')
def admin():
    if 'user_id' not in session or session['user_role'] != 'admin':
        flash('Admin access required', 'error')
        return redirect(url_for('dashboard'))
    
    tables = {
        "users": "SELECT * FROM users",
        "products": "SELECT * FROM products",
        "transactions": "SELECT * FROM transactions",
        "transaction_items": "SELECT * FROM transaction_items",
        "recommendations": "SELECT * FROM recommendations",
        "alerts": "SELECT * FROM alerts",
        "coupons": "SELECT * FROM coupons"
    }
    return render_template('admin.html', tables=tables.keys(), user=session)

@app.route('/api/admin/<table>')
def admin_table(table):
    tables = {
        "users": "SELECT * FROM users",
        "products": "SELECT * FROM products",
        "transactions": "SELECT * FROM transactions",
        "transaction_items": "SELECT * FROM transaction_items",
        "recommendations": "SELECT * FROM recommendations",
        "alerts": "SELECT * FROM alerts",
        "coupons": "SELECT * FROM coupons"
    }
    if table in tables:
        df = query_df(tables[table])
        return jsonify(df.to_dict('records'))
    return jsonify({'error': 'Table not found'}), 404

@app.route('/api/admin/delete', methods=['POST'])
def admin_delete():
    data = request.json
    table = data['table']
    pk_map = {
        "users": "id",
        "products": "product_id",
        "transactions": "transaction_id",
        "transaction_items": "item_id",
        "recommendations": "rec_id",
        "alerts": "alert_id",
        "coupons": "coupon_code"
    }
    
    if table in pk_map:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute(f"DELETE FROM {table} WHERE {pk_map[table]} = ?", (data['id'],))
        conn.commit()
        conn.close()
        return jsonify({'success': True})
    return jsonify({'error': 'Invalid table'}), 400

@app.route('/api/regenerate_data', methods=['POST'])
def regenerate_data():
    if 'user_id' not in session:
        return jsonify({'error': 'Please login before regenerating data.'}), 401

    try:
        init_db()
        generate_demo_data(force=True)
        return jsonify({'success': True})
    except Exception as exc:
        app.logger.exception("Dataset regeneration failed")
        return jsonify({'error': f'Dataset regeneration failed: {exc}'}), 500

if __name__ == '__main__':
    init_db()
    generate_demo_data()
    app.run(debug=True, port=5000)
