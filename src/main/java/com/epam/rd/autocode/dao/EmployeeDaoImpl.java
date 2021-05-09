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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

public class EmployeeDaoImpl implements EmployeeDao {

    private static final String SELECT_QUERY = "select * from EMPLOYEE";
    private static final String ID = "ID";
    private static final String FIRST_NAME = "FIRSTNAME";
    private static final String LAST_NAME = "LASTNAME";
    private static final String MIDDLE_NAME = "MIDDLENAME";
    private static final String POSITION = "POSITION";
    private static final String HIREDATE = "HIREDATE";
    private static final String SALARY = "SALARY";
    private static final String MANAGER = "MANAGER";
    private static final String DEPARTMENT = "DEPARTMENT";
    private final Map<BigInteger, Employee> employeesByMap = new HashMap<>();

    @Override
    public Optional<Employee> getById(BigInteger Id) {
        putEmployeesToMap();
        return Optional.ofNullable(employeesByMap.get(Id));
    }

    @Override
    public List<Employee> getAll() {
        putEmployeesToMap();
        return new ArrayList<>(employeesByMap.values());
    }

    @Override
    public Employee save(Employee employee) {
        employeesByMap.put(employee.getId(), employee);
        return employeesByMap.get(employee.getId());
    }

    @Override
    public void delete(Employee employee) {
        employeesByMap.remove(employee.getId());
    }

    @Override
    public List<Employee> getByDepartment(Department department) {
        putEmployeesToMap();
        return employeesByMap.values().stream()
                .filter(employee -> employee.getDepartmentId().equals(department.getId()))
                .collect(Collectors.toList());
    }

    @Override
    public List<Employee> getByManager(Employee employee) {
        putEmployeesToMap();
        return employeesByMap.values().stream()
                .filter(employeeByMap -> employeeByMap.getManagerId().equals(employee.getId()))
                .collect(Collectors.toList());
    }

    private void putEmployeesToMap() {
        try {
            Connection connection = ConnectionSource.instance().createConnection();
            Statement statement = connection.createStatement();
            ResultSet rs = statement.executeQuery(SELECT_QUERY);
            while (rs.next()) {
                employeesByMap.put(
                        new BigInteger(String.valueOf(rs.getString(ID))),
                        new Employee(
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
    }
}