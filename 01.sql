--Display film id, title, and film category name
SELECT f.film_id, f.title, c.name AS category_name
FROM film f
JOIN film_category fc ON f.film_id = fc.film_id
JOIN category c ON fc.category_id = c.category_id;


--Display film count by category name.
SELECT c.name AS category_name, COUNT(f.film_id) AS film_count
FROM film f
JOIN film_category fc ON f.film_id = fc.film_id
JOIN category c ON fc.category_id = c.category_id
GROUP BY c.name
ORDER BY film_count DESC;


--Display the number of films an actor is part of in order where actors that have done
most films first.
SELECT a.actor_id, a.first_name, a.last_name, COUNT(fa.film_id) AS film_count
FROM actor a
JOIN film_actor fa ON a.actor_id = fa.actor_id
GROUP BY a.actor_id, a.first_name, a.last_name
ORDER BY film_count DESC;

--Find how many copies of a certain film a store has
SELECT i.store_id, i.film_id, COUNT(i.inventory_id) 
FROM inventory i
GROUP BY i.store_id, i.film_id
ORDER BY i.store_id, i.film_id;


--Display list of all dvds that are rented out.
SELECT r.rental_id, f.title, i.inventory_id, r.rental_date, r.return_date
FROM rental r
JOIN inventory i ON r.inventory_id = i.inventory_id
JOIN film f ON i.film_id = f.film_id
WHERE r.return_date IS NULL;


--Display film id, title, category and rental count of the top 5 rented films
SELECT f.film_id, f.title, c.name AS category, COUNT(r.rental_id) AS rental_count
FROM rental r
JOIN inventory i ON r.inventory_id = i.inventory_id
JOIN film f ON i.film_id = f.film_id
JOIN film_category fc ON f.film_id = fc.film_id
JOIN category c ON fc.category_id = c.category_id
GROUP BY f.film_id, f.title, c.name
ORDER BY rental_count DESC
LIMIT 5;

--Display film id, title, rental count of the top 5 rented movies of the actor who has done
the most films.
SELECT f.film_id, f.title, COUNT(r.rental_id) AS rental_count
FROM rental r
JOIN inventory i ON r.inventory_id = i.inventory_id
JOIN film f ON i.film_id = f.film_id
JOIN film_actor fa ON f.film_id = fa.film_id
WHERE fa.actor_id = (
    SELECT a.actor_id
    FROM actor a
    JOIN film_actor fa2 ON a.actor_id = fa2.actor_id
    GROUP BY a.actor_id
    ORDER BY COUNT(fa2.film_id) DESC
    LIMIT 1
)
GROUP BY f.film_id, f.title
ORDER BY rental_count DESC
LIMIT 5;

--How many dvds has a customer rented? Display their first name, last name along with
rental count.
SELECT c.first_name, c.last_name, COUNT(r.rental_id) AS rental_count
FROM customer c
JOIN rental r ON c.customer_id = r.customer_id
GROUP BY c.customer_id, c.first_name, c.last_name
ORDER BY rental_count DESC;
