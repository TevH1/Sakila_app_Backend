from flask import Flask, render_template, request, jsonify
import mysql.connector

app = Flask(__name__)




def get_db_connection():
    return mysql.connector.connect(
        host="localhost",
        user="sakila_user",
        password="",
        database="sakila"
    )


@app.route("/")
def home():
    return """
    <h1>Welcome to Sakila Flask App</h1>
    <p>Available routes:</p>
    <ul>
        <li><a href='/films'>Films</a></li>
        <li><a href='/categories'>Categories</a></li>
        <li><a href='/actors'>Actors</a></li>
        <li><a href='/customers'>Customers</a></li>
    </ul>
    """

@app.route("/films")
def films():
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    cursor.execute("""
        SELECT f.film_id, f.title, c.name AS category_name
        FROM film f
        JOIN film_category fc ON f.film_id = fc.film_id
        JOIN category c ON fc.category_id = c.category_id
        LIMIT 20;
    """)
    films = cursor.fetchall()
    cursor.close()
    conn.close()
    return jsonify(films)

@app.route("/categories")
def categories():
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    cursor.execute("""
        SELECT c.name AS category_name, COUNT(f.film_id) AS film_count
        FROM film f
        JOIN film_category fc ON f.film_id = fc.film_id
        JOIN category c ON fc.category_id = c.category_id
        GROUP BY c.name
        ORDER BY film_count DESC;
    """)
    results = cursor.fetchall()
    cursor.close()
    conn.close()
    return jsonify(results)




@app.route("/top-rented")
def top_rented():
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    cursor.execute("""
        SELECT f.film_id, f.title, COUNT(r.rental_id) AS rental_count
        FROM rental r
        JOIN inventory i ON r.inventory_id = i.inventory_id
        JOIN film f ON i.film_id = f.film_id
        GROUP BY f.film_id, f.title
        ORDER BY rental_count DESC
        LIMIT 5;
    """)
    results = cursor.fetchall()
    cursor.close()
    conn.close()
    return jsonify(results)



if __name__ == "__main__":
    app.run(port=5002, debug=True)
