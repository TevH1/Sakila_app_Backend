from flask import Flask, render_template, request, jsonify
import mysql.connector
import os

base_dir = os.path.dirname(__file__)

frontend_dir = os.path.abspath(os.path.join(base_dir, "../frontend"))


app = Flask(__name__,  template_folder=os.path.join(frontend_dir, "templates"),
    static_folder=os.path.join(frontend_dir, "static"))




def get_db_connection():
    return mysql.connector.connect(
        host="localhost",
        user="sakila_user",
        password="",
        database="sakila"
    )


@app.route("/")
def landing_page():
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

    return render_template("landing.html", films=films, actors=actors)


@app.route("/actors")
def actors():
    limit = request.args.get("limit", default=5, type=int)

    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)

    query = """
        SELECT a.actor_id, a.first_name, a.last_name,
               COUNT(fa.film_id) AS film_count
        FROM actor a
        JOIN film_actor fa ON a.actor_id = fa.actor_id
        GROUP BY a.actor_id, a.first_name, a.last_name
        ORDER BY film_count DESC
        LIMIT %s;
    """
    cursor.execute(query, (limit,))
    actors = cursor.fetchall()

    cursor.close()
    conn.close()

    return jsonify(actors)



@app.route("/actor/<int:actor_id>")
def actor_detail(actor_id):
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)

    # Get actor info + films they appeared in
    cursor.execute("""
        SELECT a.actor_id, a.first_name, a.last_name,
               f.film_id, f.title, c.name AS category
        FROM actor a
        JOIN film_actor fa ON a.actor_id = fa.actor_id
        JOIN film f ON fa.film_id = f.film_id
        JOIN film_category fc ON f.film_id = fc.film_id
        JOIN category c ON fc.category_id = c.category_id
        WHERE a.actor_id = %s
        ORDER BY f.title;
    """, (actor_id,))
    rows = cursor.fetchall()

    cursor.close()
    conn.close()

    if not rows:
        return jsonify({"error": "Actor not found"}), 404

    # Group results
    actor_info = {
        "actor_id": rows[0]["actor_id"],
        "first_name": rows[0]["first_name"],
        "last_name": rows[0]["last_name"],
        "films": [{"film_id": r["film_id"], "title": r["title"], "category": r["category"]}
                  for r in rows]
    }
    return jsonify(actor_info)





@app.route("/films")
def films_page():
    search = request.args.get("search", "")
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)

    query = """
        SELECT DISTINCT f.film_id, f.title, c.name AS category, COUNT(r.rental_id) AS rental_count
        FROM film f
        JOIN film_category fc ON f.film_id = fc.film_id
        JOIN category c ON fc.category_id = c.category_id
        LEFT JOIN inventory i ON f.film_id = i.film_id
        LEFT JOIN rental r ON i.inventory_id = r.inventory_id
        LEFT JOIN film_actor fa ON f.film_id = fa.film_id
        LEFT JOIN actor a ON fa.actor_id = a.actor_id
        WHERE f.title LIKE %s OR a.first_name LIKE %s OR a.last_name LIKE %s OR c.name LIKE %s
        GROUP BY f.film_id, f.title, c.name
        ORDER BY rental_count DESC;
    """
    like = f"%{search}%"
    cursor.execute(query, (like, like, like, like))
    films = cursor.fetchall()

    cursor.close()
    conn.close()

    return render_template("films.html", films=films)




@app.route("/film/<int:film_id>")
def film_detail(film_id):
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

    cursor.close()
    conn.close()

    if not film:
        return jsonify({"error": "Film not found"}), 404
    return jsonify(film)


@app.route("/rent/<int:film_id>", methods=["POST"])
def rent_film(film_id):
    customer_id = request.form.get("customer_id")  

    conn = get_db_connection()
    cursor = conn.cursor()

    # Find an available copy of the film
    cursor.execute("SELECT inventory_id FROM inventory WHERE film_id = %s LIMIT 1;", (film_id,))
    inventory = cursor.fetchone()

    if not inventory:
        return "No copies available", 400

    inventory_id = inventory[0]

    # Insert rental
    cursor.execute("""
        INSERT INTO rental (rental_date, inventory_id, customer_id, staff_id)
        VALUES (NOW(), %s, %s, 1);
    """, (inventory_id, customer_id))
    conn.commit()

    cursor.close()
    conn.close()

    return f"Film {film_id} rented to customer {customer_id}!"





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
