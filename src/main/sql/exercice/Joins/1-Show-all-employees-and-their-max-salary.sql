-- Get the max salary for each employee
CREATE TEMP TABLE maxSalaryByEmployee AS
SELECT emp_no, MAX(salary) FROM salaries
GROUP BY emp_no;

-- Show all employees and their max salary
SELECT * FROM employees
INNER JOIN maxSalaryByEmployee mSBE on employees.emp_no = mSBE.emp_no;
--where employees.emp_no = 12940;
