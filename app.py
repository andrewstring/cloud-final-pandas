from flask import Flask, render_template, request, url_for, flash, redirect
from data.blob_access import run, run_files
from users.users import user_add, user_login

import os
from werkzeug.utils import secure_filename
UPLOAD_FOLDER = "/data/uploads"
ALLOWED_EXTENSIONS = {'csv'}


app = Flask(__name__)
app.config['SECRET_KEY'] = 'abcdefg'
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER


@app.route("/")
def entry_point():
    return render_template("/auth/entry-point.html")

@app.route("/login", methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        username = request.form['username']
        password = request.form['password']

        result = user_login(username, password)
        if result == 2:
            return redirect(url_for("selection", uname=username))
        else:
            return render_template("/auth/login.html", res=result) 

    return render_template("/auth/login.html")

@app.route("/register", methods=('GET', 'POST'))
def register():
    if request.method == 'POST':
        username = request.form['username']
        password = request.form['password']
        email = request.form['email']

        result = user_add(username, password, email)
        if result == 0:
            return render_template("/auth/register.html", res=result)
        else:
            return redirect(url_for("login"))

    return render_template("/auth/register.html")

@app.route("/selection")
def selection():
    return render_template("/display/selection.html")

@app.route("/display-10")
def display_ten():
    elements = run(10)
    print(elements)
    return render_template("/display/display-table.html", elem=elements, num=10)

@app.route("/display-selection", methods=('GET','POST'))
def display_selection():
    if request.method == "POST":
        houseNum = request.form['houseNum']

        return redirect(url_for('display_selected_num', num=houseNum))

    return render_template("/display/house-selection.html")

@app.route("/display-selected-num")
def display_selected_num():
    print(request.args.get("num"))
    elements = run(int(request.args.get("num")))
    return render_template("/display/display-table.html", elem=elements, num=request.args.get("num"))


@app.route("/upload-data", methods=('GET','POST'))
def upload_data():
    if request.method == "POST":
        household = request.files['household']
        filename = secure_filename(household.filename)
        path = os.path.join(os.getcwd(), "data/uploads/", filename)
        household.save(path)

        product = request.files['product']
        filename = secure_filename(product.filename)
        path = os.path.join(os.getcwd(), "data/uploads/", filename)
        product.save(path)

        transaction = request.files['transaction']
        filename = secure_filename(transaction.filename)
        path = os.path.join(os.getcwd(), "data/uploads/", filename)
        transaction.save(path)

        houseNum = request.form["houseNum"]
        print(houseNum)

        return redirect(url_for('display_uploaded', household=household.filename,
        product=product.filename, transaction=transaction.filename, houseNum=houseNum))
            
    return render_template("/upload/upload-data.html")

@app.route("/display-uploaded")
def display_uploaded():
    # print(request.args['household'])
    # print(request.args['product'])
    # print(request.args['transaction'])
    # return render_template("/upload/display-uploaded.html")
    files = [
        request.args['household'],
        request.args['product'],
        request.args['transaction'],
    ]
    elements = run_files(files, int(request.args['houseNum']))
    return render_template("/display/display-table.html", elem=elements, num=request.args.get("num"))

if __name__ == "__main__":
    app.run()