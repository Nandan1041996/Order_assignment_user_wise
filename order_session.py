# Necessary Imports

import shutil
import zipfile
import io
import logging
import smtplib
import random
import os
import gc
import re
import pandas as pd
import secrets
import psycopg2
from werkzeug.utils import secure_filename
from order import order_assignment_func
from flask import Flask, request, flash, redirect, send_file, render_template, url_for, jsonify, session
from exception import DataNotAvailable
import redis
import base64
from flask_session import Session
from flask import session
from sqlalchemy import create_engine
from flask_sqlalchemy import SQLAlchemy
from exception import *
import time

# Initialize logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.ERROR)
handler = logging.FileHandler('error.log')
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

app = Flask(__name__, template_folder='templates')

# IMPORTANT: Keep a fixed secret key (not random on each restart)
DEFAULT_SECRET = "A5f9Xx2ZkL8pQ3sR9mNt4Vb7Yc1HjKe"
app.secret_key = os.environ.get("FLASK_SECRET_KEY", DEFAULT_SECRET)
app.config["SECRET_KEY"] = app.secret_key

# SESSION: use Redis (server-side sessions)
# Choose a Redis DB that is free (you said db0 used by another app -> use db=2)
# redis_host = os.environ.get("REDIS_HOST", "localhost")
# redis_port = int(os.environ.get("REDIS_PORT", 6379))
# redis_db = int(os.environ.get("REDIS_DB", 2))

app.config["SESSION_TYPE"] = "redis"
app.config["SESSION_PERMANENT"] = False
app.config["SESSION_USE_SIGNER"] = True
app.config["SESSION_KEY_PREFIX"] = "flask_session:"
app.config["SESSION_REDIS"] = redis.StrictRedis(host='128.91.51.73', port=6379, db=2, decode_responses=False)

# Initialize Flask-Session
Session(app)

# Upload folder (base). Ensure it exists
UPLOAD_FOLDER = os.environ.get("UPLOAD_FOLDER", "uploads")
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)

ALLOWED_EXTENSIONS = {'xlsx', 'xls'}

def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS


def sql_connection():
    "Establish a PostgreSQL connection."

    connection_string = 'postgres://postgres:postgres@128.91.51.73:5432/order_assognmentdb'
    connection = psycopg2.connect(connection_string)
    return connection



def create_register_table(conn):
    try:
        sql_query = """ CREATE TABLE IF NOT EXISTS public.register_table_Order_assignment
                        (
                            user_id serial PRIMARY KEY,
                            username varchar NOT NULL UNIQUE,
                            password bytea NOT NULL,
                            email_id character varying(255) NOT NULL UNIQUE,
                            datetime timestamptz not null
                        )
                    """
        curr = conn.cursor()
        curr.execute(sql_query)
        return 0
    except:
        return 'Table Not Created.'
    


@app.route('/login', methods=['GET', 'POST'])
def login_page():
    try:
        if request.method == 'POST':
            email = request.form['email']
            password = request.form['password']

            sql_query = f"select * from public.register_table_Order_assignment where email_id = '{email}';"
            connection = sql_connection()
            if connection == 'Connection Error':
                raise PgConnectionError()

            else:
                curr = connection.cursor()
                curr.execute(sql_query)
                rows  = curr.fetchall()
                connection.close()

            if  len(rows) == 0 :
                flash("Email Id Not Found.", "error")

            if len(rows) != 0 :
                if rows[0][3] != email :
                    flash("Invalid Email Id", "error")
                    return redirect(url_for('login_page'))

                decPassword = base64.b64decode(rows[0][2]).decode("utf-8")
                if password == decPassword:

                    session['email'] = email
                    app.logger.debug(f"Session data: {session.items()}")
                    return redirect(url_for('index'))
                else:
                    flash("Invalid Password", "error")
                del [decPassword]
                gc.collect()
            del email,password,sql_query,rows
            gc.collect()
        return render_template('login.html')
    except PgConnectionError as exe:
        return jsonify({'error':str(exe)}),400



def send_mail(receiver_email_id,message):
    try:
        sender_email_id = 'mayurnandanwar@ghcl.co.in'
        password = 'uvhr zbmk yeal ujhv'
        # creates SMTP session
        s = smtplib.SMTP('smtp.gmail.com', 587)
        # start TLS for security
        s.starttls()
        # Authentication
        s.login(sender_email_id, password)
        # message to be sent
        # sending the mail
        s.sendmail(sender_email_id, receiver_email_id, str(message))
        # terminating the session
        s.quit()

        del sender_email_id,password
        gc.collect()
        return 0
    except:
        return jsonify({'error':'The Message cannot be Sent.'})


@app.route('/signup', methods=['GET', 'POST'])
def signup():
    try:
        if request.method == 'POST':
            name = request.form['name']
            email = request.form['email']
            password = request.form['password']
            confirm_password = request.form['confirm_password']
            connection = sql_connection()
            table_create = create_register_table(connection)
            if table_create == 0:
                curr = connection.cursor()
                sql = f"SELECT email_id FROM public.register_table_Order_assignment WHERE email_id = '{email}';"

                curr.execute(sql)
                rows = curr.fetchall()
                connection.close()
            else:
                raise PgConnectionError()

            if len(rows) == 0:
                # Check if passwords match
                if password != confirm_password:
                    flash("Passwords do not match!", "error")
                    return redirect(url_for('signup'))

                # Check password strength using regex
                password_pattern = re.compile(r'^(?=.*[a-z])(?=.*[A-Z])(?=.*\d)(?=.*[@$!%*?&])[A-Za-z\d@$!%*?&]{8,}$')
                if not password_pattern.match(password):
                    flash("Password must contain at least 8 characters, including one uppercase letter, one lowercase letter, one digit, and one special character.", "error")
                    return redirect(url_for('signup'))

                # Generate a random token
                token = random.randint(100000, 999999)

                # Store the token in the session for validation
                session['token'] = str(token)
                session['email'] = email
                app.logger.debug(f"Session data: {session.items()}")
                # this required for adding pass and name after validation
                session['password'] = password
                session['name'] = name
                # Send the token via email
                subject = "Email Verification Code"
                body = f"Your verification code is {token}. Please enter it on the website to verify your email."
                message = f"Subject: {subject}\n\n{body}"
                msg = send_mail(email, message)

                if msg == 0:
                    flash("Code has been sent to register email id.", "info")
                    # Redirect to the validate_mail route with email as a parameter
                    return redirect(url_for('validate_mail', email=email))
                del password_pattern,token,subject,body,message,msg
                gc.collect()
            else:
                flash("Email Already Exist.", "info")
        return render_template('signup.html')
    except PgConnectionError as exe:
        return jsonify({"error": str(exe)})



@app.route('/validate_mail',methods=['POST','GET'])
def validate_mail():

    try:
        email = request.args.get('email')  # Retrieve email from query string

        if request.method == 'POST':
            entered_token = str(request.form['token'])

            # Compare the entered token with the session token
            if str(session.get('token')) == str(entered_token):
                password = session['password']
                name = session['name']
                encPassword = base64.b64encode(password.encode("utf-8"))
                connection = sql_connection()
                table_created = create_register_table(connection)
                if table_created==0:
                    datetime = time.ctime()
                    sql_query = "INSERT INTO public.register_table_Order_assignment (username, password, email_id,datetime) VALUES (%s, %s, %s,%s);"
                    curr = connection.cursor()
                    curr.execute(sql_query, (name, encPassword, email,datetime))
                    connection.commit()
                    connection.close()
                else:
                    raise ConnectionError()

                #remove session after adding it to table
                session.pop('password')
                session.pop('name')
                session.pop('token')

                flash("Signup successful! Please login.", "success")
                del password,name,encPassword,sql_query
                gc.collect()
                return redirect(url_for('login_page'))
            else:
                # return "Invalid token. Please try again.", 400
                flash("Invalid code. Please try again.", "error")  # Flash error message

        return render_template('validate_mail.html', email=email)

    except ConnectionError as exe:
        return jsonify({'error': str(exe)}),400

@app.route('/validate_mail_reset_password',methods=['POST','GET'])
def validate_mail_reset_password():
    email = request.args.get('email')  # Retrieve email from query string
    if request.method == 'POST':
        entered_token = str(request.form['token'])

        # Compare the entered token with the session token
        if str(session.get('reset_token')) == str(entered_token):
            return redirect(url_for('reset_password'))
        else:
            # return "Invalid token. Please try again.", 400
            flash("Invalid code. Please try again.", "error")  # Flash error message

    return render_template('reset_token_validate.html', email=email)


@app.route('/reset_password', methods=['GET', 'POST'])
def reset_password():
    try:
        if request.method == 'POST':
            email = session['email']
            new_password = request.form['new_password']
            confirm_password = request.form['confirm_password']
            password_pattern = re.compile(r'^(?=.*[a-z])(?=.*[A-Z])(?=.*\d)(?=.*[@$!%*?&])[A-Za-z\d@$!%*?&]{8,}$')
            if not password_pattern.match(new_password):
                flash("Password must contain at least 8 characters, including one uppercase letter, one lowercase letter, one digit, and one special character.", "error")
                return render_template('reset_password.html')

            if new_password != confirm_password:
                flash("Passwords do not match.", "error")
                return render_template('reset_password.html')
            # Check password strength using regex
            else:
                encPassword = base64.b64encode(new_password.encode("utf-8"))
                sql_query = "UPDATE public.register_table_Order_assignment SET password = %s WHERE email_id = %s;"
                connection = sql_connection()
                if connection == 'Connection Error':
                    raise PgConnectionError()
                else:
                    curr = connection.cursor()
                    curr.execute(sql_query,(encPassword,email))
                    connection.commit()
                    connection.close()

                    flash("Password has been reset successfully. You can now log in.", "success")
                del encPassword,sql_query
                gc.collect()
              
                return redirect(url_for('login_page'))
        return render_template('reset_password.html')
    except PgConnectionError as exe:
        return jsonify({'error':str(exe)})


@app.route('/forgot_password', methods=['GET', 'POST'])
def forgot_password():
    try:
        if request.method == 'POST':
            email = request.form['email']
            session['email'] = email
            sql_query = f"SELECT * FROM public.register_table_Order_assignment WHERE email_id = '{email}';"
            connection = sql_connection()
            if connection == 'Connection Error':
                raise PgConnectionError()
            else:
                curr = connection.cursor()
                curr.execute(sql_query)
                rows = curr.fetchall()
                connection.close()

            if len(rows) == 0:
                flash("Email not found. Please SignUp", "error")
                return redirect(url_for('signup'))
            else:
                # Generate a random token
                reset_token = str(random.randint(100000, 999999))
                session['reset_token'] = reset_token
                subject = "Code For Password Change"
                body = f"Your verification code is {reset_token}. Please enter it on the website to verify your email."
                message = f"Subject: {subject}\n\n{body}"
                msg = send_mail(email, message)

                del reset_token,subject,body,message
                gc.collect()

                if msg == 0:
                    flash("Code has been sent to registered email id.", "info")
                    return redirect(url_for('validate_mail_reset_password', email=email))

            del email,sql_query,rows
            gc.collect()
        return render_template('forgot_password.html')

    except PgConnectionError as exe:
        return jsonify({'error':str(exe)})

@app.route('/')
def index():
    return render_template('index.html')


@app.route('/upload', methods=['POST','GET'])
def upload_files():
    try:
        if 'email' not in session:
            app.logger.debug(f"Session data: {session.items()}")
            flash("Please log in to access this functionality.", "error")
            return redirect(url_for('login_page'))
        
        # if method is get then we will get data from session 
        if request.method == "GET":
            email = session.get("email")

            if not email:
                return "Email not found in session", 400

            # Condition for DataFrame stored as JSON
            if session.get(f"{email}_result_df"):
                result_df_json = session[f"{email}_result_df"]
                result_df = pd.read_json(result_df_json)
            else:
                result_df = None

            # Condition for unique sales list
            if session.get(f"{email}_unique_sales"):
                unique_sales = session[f"{email}_unique_sales"]
            else:
                unique_sales = None

            return render_template('result_page.html', sales_orders=unique_sales, result_df=result_df)

        else:

            email = session['email']
        
            # Validate files
            if 'pending_order_file' not in request.files or 'rate_file' not in request.files or 'stock_file' not in request.files:
                flash('No file part')
                return redirect(request.url)

            pending_order_file = request.files['pending_order_file']
            rate_file = request.files['rate_file']
            stock_file = request.files['stock_file']

            if not (allowed_file(pending_order_file.filename) and
                    allowed_file(rate_file.filename) and
                    allowed_file(stock_file.filename)):
                flash('Invalid file format. Please upload Excel files only.')
                return redirect(request.url)

            # Create unique folder for this request / user
            req_id = secrets.token_hex(8)
            user_folder = os.path.join(app.config['UPLOAD_FOLDER'], req_id)
            os.makedirs(user_folder, exist_ok=True)

            # Save files into the unique user folder
            filename1 = secure_filename(pending_order_file.filename)
            filename2 = secure_filename(rate_file.filename)
            filename3 = secure_filename(stock_file.filename)

            path1 = os.path.join(user_folder, filename1)
            path2 = os.path.join(user_folder, filename2)
            path3 = os.path.join(user_folder, filename3)

            pending_order_file.save(path1)
            rate_file.save(path2)
            stock_file.save(path3)

            # Run core logic
            result_df1, rate_stck_df_c1 = order_assignment_func(path1, path2, path3)

            # Error handling from your function
            if not isinstance(result_df1, pd.DataFrame):
                # cleanup user folder
                try:
                    shutil.rmtree(user_folder)
                except Exception:
                    pass
                return render_template('error_page.html', error_message=str(result_df1))

            # Clean DataFrame (select & rename columns)
            result_df1 = result_df1[['Salse Order', 'Sold to', 'Customer Name', 'order_plant', 'Plant',
                                    'Plant Zone', 'Plant Zone Desc', 'Final Destination', 'Dest. Desc.',
                                    'Route Name', 'Disp. Date', 'MODE', 'Material', 'Required_Stock',
                                    'Total with STO', 'Proposed Level', 'confirm']]

            result_df1.rename(columns={
                'Sold to': 'Sold To',
                'order_plant': 'Ordered Plant',
                'Plant': 'Suggested Plant',
                'confirm': 'Order Confirmed',
                'Total with STO': 'Trans Cost',
                'Required_Stock': 'Ordered Quantity'
            }, inplace=True)

            # Store DF in server-side session (Redis)
            # store user folder so we know where files live for download

            # Create sales order dropdown values
            unique_sales_orders = [int(i) for i in result_df1['Salse Order'].unique()]

            # Save excel outputs inside user_folder
            file1_path = os.path.join(user_folder, 'order_with_route.xlsx')
            result_df1.to_excel(file1_path, index=False)

            updated_stock_path = os.path.join(user_folder, 'updated_stock_file.xlsx')
            rate_stck_df_c1.to_excel(updated_stock_path, index=False)
            session[email+'_result_df'] = result_df1.to_json()
            session[email+'_unique_sales'] = unique_sales_orders
            
            return render_template('result_page.html', sales_orders=unique_sales_orders, result_df=result_df1)

    except Exception as e:
        logger.error(f"Exception occurred in /upload: {str(e)}", exc_info=True)
        return jsonify({'error': 'Please ensure your uploaded files include all required columns.'}), 500


@app.route('/process_orders', methods=['GET'])
def process_orders():
    try:

        if 'email' not in session:
            flash("Please log in to access this functionality.", "error")
            return redirect(url_for('login_page'))
        email = session['email']
        print('email:',email)
        result_df1 = pd.read_json(session[email+'_result_df'])
        
        print('type:',type(result_df1))
        selected_orders = request.args.get('orders')

        if not selected_orders:
            return "No sales orders selected."

        # Split selected orders into a list
        selected_orders = selected_orders.split(',')
        selected_orders = [float(i) for i in selected_orders]

        # Process selected orders using result_df1
        if result_df1 is not None:
            print('in')
            print('resdf',result_df1)
            filtered_df = result_df1[result_df1['Salse Order'].isin(selected_orders)]
            #  Reset index starting from 1
            filtered_df.reset_index(drop=True, inplace=True)
            filtered_df.index = filtered_df.index + 1
             # Save DataFrame to CSV
            csv_path = os.path.join(app.config['UPLOAD_FOLDER'], 'Selected Orders.csv')
            filtered_df.to_csv(csv_path, index=False)

            # Render the template with the filtered DataFrame
            return render_template('result_display.html', filtered_df=filtered_df.to_html(classes='table table-striped'))
        else:
            raise DataNotAvailable()


    except (DataNotAvailable) as e:
        logger.error(f"Exception occurred: {str(e)}")
        return render_template(f'''error_page.html', error_message={e}''')

    except Exception as e:
        logger.error(f"Exception occurred: {str(e)}")
        return jsonify({'error': 'Data Not Found.'}), 500



@app.route('/download_csv_trigger', methods=['GET'])
def download_csv_trigger():
    # Trigger the download of the CSV file
    return redirect(url_for('download_csv'))

@app.route('/download_csv', methods=['GET'])
def download_csv():
    try:
        csv_path = os.path.join(app.config['UPLOAD_FOLDER'], 'Selected Orders.csv')
        if os.path.exists(csv_path):
            return send_file(csv_path, mimetype='text/csv', as_attachment=True, download_name='Selected Orders.csv')
        else:
            return jsonify({'error': 'File not found'}), 404
    except Exception as e:
        return jsonify({'error': f'An unexpected error occurred: {str(e)}'}), 500


if __name__ == '__main__':
    # For production use gunicorn instead of app.run
    app.run(debug=True)
