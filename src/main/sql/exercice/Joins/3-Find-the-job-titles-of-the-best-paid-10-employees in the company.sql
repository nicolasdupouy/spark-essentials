-- 10 best paid job titles
select titles.title, max(salary) from salaries
inner join titles on salaries.emp_no = titles.emp_no
group by titles.title
order by max(salary) desc
limit 10;

-- Job titles of the best paid 10 employees in the company
