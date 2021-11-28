SELECT * from dept_manager;

SELECT count(1) from employees;

SELECT count(1) from employees
LEFT OUTER JOIN dept_manager dm on employees.emp_no = dm.emp_no
WHERE dm.emp_no IS NULL;

