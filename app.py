from flask import Flask, request, jsonify
from flask_cors import CORS
import mysql.connector
import os

base_dir = os.path.dirname(__file__)

frontend_dir = os.path.abspath(os.path.join(base_dir, "../frontend"))


app = Flask(__name__)
CORS(app)



def get_db_connection():
    return mysql.connector.connect(
        host="localhost",
        user="sakila_user",
        password="",
        database="sakila"
    )


@app.route("/api/landing")
def landing_api():
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
    films = cursor.fetchall()

    cursor.execute("""
        SELECT a.actor_id, a.first_name, a.last_name, COUNT(fa.film_id) AS film_count
        FROM actor a
        JOIN film_actor fa ON a.actor_id = fa.actor_id
        GROUP BY a.actor_id, a.first_name, a.last_name
        ORDER BY film_count DESC
        LIMIT 5;
    """)
    actors = cursor.fetchall()

    cursor.close()
    conn.close()

    return jsonify({"films": films, "actors": actors})


@app.route("/api/actors")
def actors_api():
    page = request.args.get("page", default=1, type=int)
    per_page = request.args.get("per_page", default=10, type=int)
    offset = (page - 1) * per_page

    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)

    query = """
        SELECT a.actor_id, a.first_name, a.last_name,
               COUNT(fa.film_id) AS film_count
        FROM actor a
        JOIN film_actor fa ON a.actor_id = fa.actor_id
        GROUP BY a.actor_id, a.first_name, a.last_name
        ORDER BY film_count DESC
        LIMIT %s OFFSET %s;
    """
    cursor.execute(query, (per_page, offset))
    actors = cursor.fetchall()

    cursor.execute("SELECT COUNT(*) AS total FROM actor;")
    total = cursor.fetchone()["total"]

    cursor.close()
    conn.close()

    return jsonify({
        "actors": actors,
        "page": page,
        "per_page": per_page,
        "total": total
    })



@app.route("/api/actors/<int:actor_id>")
def actor_detail_api(actor_id):
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)

    cursor.execute("""
        SELECT a.actor_id, a.first_name, a.last_name, COUNT(fa.film_id) AS film_count
        FROM actor a
        JOIN film_actor fa ON a.actor_id = fa.actor_id
        WHERE a.actor_id = %s
        GROUP BY a.actor_id, a.first_name, a.last_name;
    """, (actor_id,))
    actor = cursor.fetchone()

    cursor.execute("""
        SELECT f.film_id, f.title, COUNT(r.rental_id) AS rental_count
        FROM film f
        JOIN film_actor fa ON f.film_id = fa.film_id
        JOIN inventory i ON f.film_id = i.film_id
        LEFT JOIN rental r ON i.inventory_id = r.inventory_id
        WHERE fa.actor_id = %s
        GROUP BY f.film_id, f.title
        ORDER BY rental_count DESC
        LIMIT 5;
    """, (actor_id,))
    films = cursor.fetchall()

    cursor.close()
    conn.close()


    if not actor:
        return jsonify({"error": "Actor not found"}), 404
    return jsonify({"actor": actor, "films": films})



@app.route("/api/films")
def films_api():
    search = request.args.get("search", "")
    page = request.args.get("page", default=1, type=int)
    per_page = request.args.get("per_page", default=10, type=int)
    offset = (page - 1) * per_page

    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)

    query = """
        SELECT f.film_id, f.title, c.name AS category,
               (SELECT COUNT(*) 
                FROM inventory i
                LEFT JOIN rental r2 ON i.inventory_id = r2.inventory_id AND r2.return_date IS NULL
                WHERE i.film_id = f.film_id AND r2.rental_id IS NULL) AS available_copies
        FROM film f
        JOIN film_category fc ON f.film_id = fc.film_id
        JOIN category c ON fc.category_id = c.category_id
        LEFT JOIN film_actor fa ON f.film_id = fa.film_id
        LEFT JOIN actor a ON fa.actor_id = a.actor_id
        WHERE f.title LIKE %s OR a.first_name LIKE %s OR a.last_name LIKE %s OR c.name LIKE %s
        GROUP BY f.film_id, f.title, c.name
        ORDER BY f.title
        LIMIT %s OFFSET %s;
    """
    like = f"%{search}%"
    cursor.execute(query, (like, like, like, like, per_page, offset))
    films = cursor.fetchall()

    cursor.execute("""
        SELECT COUNT(DISTINCT f.film_id) AS total
        FROM film f
        JOIN film_category fc ON f.film_id = fc.film_id
        JOIN category c ON fc.category_id = c.category_id
        LEFT JOIN film_actor fa ON f.film_id = fa.film_id
        LEFT JOIN actor a ON fa.actor_id = a.actor_id
        WHERE f.title LIKE %s OR a.first_name LIKE %s OR a.last_name LIKE %s OR c.name LIKE %s;
    """, (like, like, like, like))
    total = cursor.fetchone()["total"]

    cursor.close()
    conn.close()

    return jsonify({
        "films": films,
        "page": page,
        "per_page": per_page,
        "total": total
    })




@app.route("/api/film/<int:film_id>")
def film_detail_api(film_id):
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)

    cursor.execute("""
        SELECT f.film_id, f.title, f.description, f.release_year, 
               c.name AS category, COUNT(r.rental_id) AS rental_count
        FROM film f
        JOIN film_category fc ON f.film_id = fc.film_id
        JOIN category c ON fc.category_id = c.category_id
        LEFT JOIN inventory i ON f.film_id = i.film_id
        LEFT JOIN rental r ON i.inventory_id = r.inventory_id
        WHERE f.film_id = %s
        GROUP BY f.film_id, f.title, f.description, f.release_year, c.name;
    """, (film_id,))
    film = cursor.fetchone()

    cursor.execute("""
        SELECT a.actor_id, a.first_name, a.last_name
        FROM actor a
        JOIN film_actor fa ON a.actor_id = fa.actor_id
        WHERE fa.film_id = %s
        LIMIT 5;
    """, (film_id,))
    actors = cursor.fetchall()

    cursor.close()
    conn.close()

    if not film:
        return jsonify({"error": "Film not found"}), 404

    return jsonify({"film": film, "actors": actors})



@app.route("/api/rent/<int:film_id>", methods=["POST"])
def rent_film(film_id):
    customer_id = request.json.get("customer_id")

    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)

    cursor.execute("""
        SELECT i.inventory_id
        FROM inventory i
        LEFT JOIN rental r
          ON i.inventory_id = r.inventory_id
          AND r.return_date IS NULL
        WHERE i.film_id = %s
        AND r.rental_id IS NULL
        LIMIT 1;
    """, (film_id,))
    inventory = cursor.fetchone()

    if not inventory:
        cursor.close()
        conn.close()
        return "No copies available to rent", 400

    inventory_id = inventory["inventory_id"]

    cursor.execute("""
        INSERT INTO rental (rental_date, inventory_id, customer_id, staff_id)
        VALUES (NOW(), %s, %s, 1);
    """, (inventory_id, customer_id))
    conn.commit()

    cursor.close()
    conn.close()

    return jsonify({"success": True, "customer_id": customer_id, "film_id": film_id})



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




@app.route("/api/top-rented")
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


@app.route("/api/customers")
def customers_api():
    page = request.args.get("page", 1, type=int)
    per_page = 10
    offset = (page - 1) * per_page
    search = request.args.get("search", "")

    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)

    query = """
        SELECT customer_id, first_name, last_name, email
        FROM customer
        WHERE customer_id LIKE %s OR first_name LIKE %s OR last_name LIKE %s
        ORDER BY last_name
        LIMIT %s OFFSET %s;
    """
    like = f"%{search}%"
    cursor.execute(query, (like, like, like, per_page, offset))
    customers = cursor.fetchall()

    cursor.execute("SELECT COUNT(*) AS count FROM customer;")
    total = cursor.fetchone()["count"]

    cursor.close()
    conn.close()

    return jsonify({
        "customers": customers,
        "page": page,
        "total": total,
        "per_page": per_page,
        "search": search
    })


@app.route("/api/customers/<int:customer_id>")
def customer_detail_api(customer_id):
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)

    cursor.execute("SELECT * FROM customer WHERE customer_id = %s;", (customer_id,))
    customer = cursor.fetchone()

    if not customer:
        cursor.close()
        conn.close()
        return f"Customer {customer_id} not found", 404

    cursor.execute("""
        SELECT r.rental_id, f.title, r.rental_date, r.return_date
        FROM rental r
        JOIN inventory i ON r.inventory_id = i.inventory_id
        JOIN film f ON i.film_id = f.film_id
        WHERE r.customer_id = %s
        ORDER BY r.rental_date DESC;
    """, (customer_id,))
    rentals = cursor.fetchall()

    cursor.close()
    conn.close()

    return jsonify({"customer": customer, "rentals": rentals})

@app.route("/api/rentals/return/<int:rental_id>", methods=["POST"])
def return_rental(rental_id):
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    cursor.execute("SELECT customer_id FROM rental WHERE rental_id = %s;", (rental_id,))
    row = cursor.fetchone()



    if not row:
        cursor.close()
        conn.close()
        return jsonify({"error": "Rental not found"}), 404

    customer_id = row["customer_id"]

    cursor.execute("UPDATE rental SET return_date = NOW() WHERE rental_id = %s;", (rental_id,))
    conn.commit()

    cursor.close()
    conn.close()

    return jsonify({"success": True, "customer_id": customer_id})

@app.route("/api/customers", methods=["POST"])
def add_customer():
    if request.method == "POST":
        first_name = request.json["first_name"]
        last_name = request.json["last_name"]
        email = request.json["email"]

        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO customer (store_id, first_name, last_name, email, address_id, create_date)
            VALUES (1, %s, %s, %s, 1, NOW());
        """, (first_name, last_name, email))
        conn.commit()
        cursor.close()
        conn.close()
        return jsonify({"success": True, "customer_id": cursor.lastrowid}), 201




@app.route("/api/customers/<int:customer_id>", methods=["PUT"])
def edit_customer(customer_id):
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)

    first_name = request.json.get("first_name")
    last_name = request.json.get("last_name")
    email = request.json.get("email")

    cursor.execute("""
        UPDATE customer
        SET first_name=%s, last_name=%s, email=%s
        WHERE customer_id=%s;
    """, (first_name, last_name, email, customer_id))
    conn.commit()
    cursor.close()
    conn.close()

    return jsonify({"success": True})


@app.route("/api/customers/<int:customer_id>", methods=["DELETE"])
def delete_customer(customer_id):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("DELETE FROM customer WHERE customer_id=%s;", (customer_id,))
    conn.commit()
    cursor.close()
    conn.close()
    return jsonify({"success": True})











if __name__ == "__main__":
    app.run(port=5002, debug=True)
