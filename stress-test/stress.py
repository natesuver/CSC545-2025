import mysql.connector
import multiprocessing
import time
#pip install multiprocessing mysql
# Database configuration (update as needed)
DB_CONFIG = {
    "host": "localhost",
    "user": "root",
    "password": "xxx",
    "database": "employees"
}

# Queries to stress test the database
QUERIES = [
    """
    SELECT e.emp_no, e.first_name, e.last_name, d.dept_name, s.salary
    FROM employees e
    JOIN salaries s ON e.emp_no = s.emp_no
    JOIN dept_emp de ON e.emp_no = de.emp_no
    JOIN departments d ON de.dept_no = d.dept_no
    WHERE s.salary > (SELECT AVG(salary) FROM salaries)
    ORDER BY s.salary DESC
    LIMIT 50000;
    """,
    """
    INSERT INTO employees (emp_no, birth_date, first_name, last_name, gender, hire_date)
    SELECT emp_no + FLOOR(RAND() * 1000000) + 1000000, birth_date, first_name, last_name, gender, hire_date 
    FROM employees 
    WHERE emp_no NOT IN (SELECT emp_no FROM employees)
    LIMIT 1000;
    """,
    """
    INSERT INTO employees (emp_no, birth_date, first_name, last_name, gender, hire_date)
    SELECT emp_no + 100000000, birth_date, first_name, last_name, gender, hire_date
    FROM employees
    WHERE NOT EXISTS (
        SELECT 1 FROM employees e2 WHERE e2.emp_no = employees.emp_no + 1000000
    )
    LIMIT 1000;
    """,
    """
    SELECT COUNT(*) FROM employees e1, employees e2;
    """
]

def run_query(query, iterations=10):
    """Runs a query multiple times to generate load."""
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor()
        for _ in range(iterations):
            start_time = time.time()
            cursor.execute(query)
            if cursor.with_rows:  # Fetch results only for SELECT queries
                cursor.fetchall()
            conn.commit()  # Required for INSERT queries
            print(f"Query executed in {time.time() - start_time:.2f} seconds")
        cursor.close()
        conn.close()
    except Exception as e:
        print(f"Error: {e}")

def simulate_users(query_index, num_users=10, iterations_per_user=5):
    """Simulates multiple users running a query concurrently."""
    query = QUERIES[query_index]
    processes = []
    
    for _ in range(num_users):
        p = multiprocessing.Process(target=run_query, args=(query, iterations_per_user))
        processes.append(p)
        p.start()

    for p in processes:
        p.join()

if __name__ == "__main__":
    print("Starting MySQL load simulation...")
    
    # Select the query you want to stress test (0 = complex join, 1 = insert with random IDs, 2 = insert with EXISTS check, 3 = cartesian join)
    query_choice = 3  # Change this to test different queries
    
    # Define the number of concurrent users (MySQL Workbench should show CPU spikes)
    num_users = 50  # Adjust to simulate different levels of load

    # Define how many times each user runs the query
    iterations_per_user = 500

    simulate_users(query_choice, num_users, iterations_per_user)
    
    print("Load simulation completed.")
