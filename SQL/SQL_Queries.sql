---------------------------------------------------------------------------------------------------#1
SELECT category.name AS cat_name, COUNT(film_id) AS cnt
FROM film_category AS fc
INNER JOIN category ON fc.category_id = category.category_id
GROUP BY(cat_name) ORDER BY cnt DESC;

---------------------------------------------------------------------------------------------------#2
SELECT actor.first_name AS act_nm, COUNT(*) AS cnt
FROM film_actor
INNER JOIN actor ON film_actor.actor_id = actor.actor_id
GROUP BY (act_nm) ORDER BY cnt DESC LIMIT 10;

---------------------------------------------------------------------------------------------------#3
SELECT c.name, SUM(p.amount) AS summ
FROM payment, category c
LEFT JOIN film_category fc ON fc.category_id = c.category_id 
LEFT JOIN film f ON fc.film_id = f.film_id
LEFT JOIN inventory iv ON iv.film_id = f.film_id
LEFT JOIN rental re ON re.inventory_id = iv.inventory_id
LEFT JOIN payment p ON p.rental_id = re.rental_id
GROUP BY c.name
LIMIT 1;

---------------------------------------------------------------------------------------------------#4
SELECT film.title
FROM film
WHERE NOT EXISTS (SELECT film_id FROM inventory WHERE film.film_id = inventory.film_id);

---------------------------------------------------------------------------------------------------#5
SELECT actor.first_name AS act, COUNT(film_actor.film_id) AS cnt
FROM actor
INNER JOIN film_actor ON actor.actor_id = film_actor.actor_id
WHERE actor.actor_id IN
(SELECT actor_id FROM film_actor WHERE film_id IN
(SELECT film_id FROM film_category WHERE category_id IN
(SELECT category_id FROM category WHERE name = 'Children'))) GROUP BY act ORDER BY cnt DESC LIMIT 3;

---------------------------------------------------------------------------------------------------#6
SELECT a.city, a.active, n.not_active FROM (SELECT city, SUM(c.customer_id) as active
FROM city ci
LEFT JOIN address ad on ad.city_id = ci.city_id
LEFT JOIN customer c on c.address_id = ad.address_id
WHERE c.active = 1
GROUP BY city) AS a
FULL JOIN (SELECT city, SUM(c.customer_id) as not_active
FROM city ci
LEFT JOIN address ad on ad.city_id = ci.city_id
LEFT JOIN customer c on c.address_id = ad.address_id
WHERE c.active = 0
GROUP BY city) AS n ON a.city = n.city;

---------------------------------------------------------------------------------------------------#7
with cte as
(SELECT c.name AS category, SUM(f.rental_duration) AS rental_duration
FROM category c
LEFT JOIN film_category fc ON fc.category_id = c.category_id 
LEFT JOIN film f ON fc.film_id = f.film_id
LEFT JOIN inventory iv ON iv.film_id = f.film_id
LEFT JOIN rental re ON iv.inventory_id = re.inventory_id
LEFT JOIN customer cu ON cu.customer_id = re.customer_id
LEFT JOIN address ad ON ad.address_id = cu.address_id
LEFT JOIN city ci ON ci.city_id = ad.city_id
GROUP BY c.name
ORDER BY rental_duration DESC)

SELECT * FROM (SELECT category, rental_duration FROM cte
WHERE category LIKE '%-%'
LIMIT 1) as a
UNION
SELECT * FROM (SELECT category, rental_duration FROM cte
WHERE category ILIKE 'A%'
LIMIT 1) as b;

















