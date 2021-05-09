package com.epam.rd.autocode.dao;

import com.epam.rd.autocode.ConnectionSource;
import com.epam.rd.autocode.domain.Department;
import com.epam.rd.autocode.domain.Employee;
import com.epam.rd.autocode.domain.FullName;
import com.epam.rd.autocode.domain.Position;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class EmployeeDaoImpl implements EmployeeDao {

    private static final String FROM_EMPLOYEE_WHERE_ID = "select * from EMPLOYEE where ID = ";
    private static final String FROM_EMPLOYEE = "select * from EMPLOYEE";
    private static final String ID = "ID";
    private static final String FIRST_NAME = "FIRSTNAME";
    private static final String LAST_NAME = "LASTNAME";
    private static final String MIDDLE_NAME = "MIDDLENAME";
    private static final String POSITION = "POSITION";
    private static final String HIREDATE = "HIREDATE";
    private static final String SALARY = "SALARY";
    private static final String MANAGER = "MANAGER";
    private static final String DEPARTMENT = "DEPARTMENT";

    @Override
    public Optional<Employee> getById(BigInteger Id) {
        return Optional.ofNullable(getEmployee(FROM_EMPLOYEE_WHERE_ID + Id));
    }

    @Override
    public List<Employee> getAll() {
        List<Employee> employees = new ArrayList<>();
        try {
            Connection connection = ConnectionSource.instance().createConnection();
            Statement statement = connection.createStatement();
            ResultSet rs = statement.executeQuery(FROM_EMPLOYEE);
            while (rs.next()) {
                employees.add(new Employee(
                                new BigInteger(String.valueOf(rs.getString(ID))),
                                new FullName(
                                        rs.getString(FIRST_NAME),
                                        rs.getString(LAST_NAME),
                                        rs.getString(MIDDLE_NAME)
                                ),
                                Position.valueOf(rs.getString(POSITION)),
                                rs.getDate(HIREDATE).toLocalDate(),
                                new BigDecimal(rs.getInt(SALARY)),
                                new BigInteger(String.valueOf(rs.getInt(MANAGER))),
                                new BigInteger(String.valueOf(rs.getInt(DEPARTMENT)))
                        )
                );
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return employees;
    }

    @Override
    public Employee save(Employee employee) {
        try {
            Connection connection = ConnectionSource.instance().createConnection();
            Statement statement = connection.createStatement();
            if (isNonExistId(employee)) {
                statement.executeUpdate(buildInsert(employee));
            } else {
                statement.executeUpdate(buildUpdate(employee));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return getEmployee(FROM_EMPLOYEE_WHERE_ID + employee.getId());
    }

    @Override
    public void delete(Employee employee) {
        try {
            Connection connection = ConnectionSource.instance().createConnection();
            Statement statement = connection.createStatement();
            statement.executeUpdate(buildDelete(employee.getId().toString()));
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Override
    public List<Employee> getByDepartment(Department department) {
        return getAll().stream()
                .filter(employee -> employee.getDepartmentId().equals(department.getId()))
                .collect(Collectors.toList());
    }

    @Override
    public List<Employee> getByManager(Employee employee) {
        return getAll().stream()
                .filter(e -> e.getManagerId().equals(employee.getId()))
                .collect(Collectors.toList());
    }

    private Employee getEmployee(String query) {
        Employee employee = null;
        try {
            Connection connection = ConnectionSource.instance().createConnection();
            Statement statement = connection.createStatement();
            ResultSet rs = statement.executeQuery(query);
            while (rs.next()) {
                employee = new Employee(
                        new BigInteger(String.valueOf(rs.getString(ID))),
                        new FullName(
                                rs.getString(FIRST_NAME),
                                rs.getString(LAST_NAME),
                                rs.getString(MIDDLE_NAME)
                        ),
                        Position.valueOf(rs.getString(POSITION)),
                        rs.getDate(HIREDATE).toLocalDate(),
                        new BigDecimal(rs.getInt(SALARY)),
                        new BigInteger(String.valueOf(rs.getInt(MANAGER))),
                        new BigInteger(String.valueOf(rs.getInt(DEPARTMENT)))
                );
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return employee;
    }

    private boolean isNonExistId(Employee employee) {
        return getAll().stream().noneMatch(dep -> dep.getId().equals(employee.getId()));
    }

    public String buildInsert(Employee employee) {
        return "insert into EMPLOYEE (ID, FIRSTNAME, LASTNAME, MIDDLENAME, POSITION, HIREDATE, SALARY, MANAGER, DEPARTMENT) " +
                "values ('" +
                employee.getId() + "', '" +
                employee.getFullName().getFirstName() + "', '" +
                employee.getFullName().getLastName() + "', '" +
                employee.getFullName().getMiddleName() + "', '" +
                employee.getPosition() + "', '" +
                employee.getHired() + "', '" +
                employee.getSalary() + "', '" +
                employee.getManagerId() + "', '" +
                employee.getDepartmentId() + "')";
    }

    public String buildUpdate(Employee employee) {
        return "update EMPLOYEE set " +
                "FIRSTNAME = '" + employee.getFullName().getFirstName() + "', " +
                "LASTNAME = '" + employee.getFullName().getLastName() + "', " +
                "MIDDLENAME = '" + employee.getFullName().getLastName() + "', " +
                "POSITION = '" + employee.getPosition() + "', " +
                "HIREDATE = '" + employee.getHired() + "', " +
                "SALARY = '" + employee.getSalary() + "', " +
                "MANAGER = '" + employee.getManagerId() + "', " +
                "DEPARTMENT = '" + employee.getDepartmentId() + "', " +
                "' where ID = '" + employee.getId() + "'";
    }

    public String buildDelete(String id) {
        return "delete from EMPLOYEE where ID = '" + id + "'";
    }
}