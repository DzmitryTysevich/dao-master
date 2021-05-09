package com.epam.rd.autocode.dao;

import com.epam.rd.autocode.ConnectionSource;
import com.epam.rd.autocode.domain.Department;

import java.math.BigInteger;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class DepartmentDaoImpl implements DepartmentDao {

    private static final String FROM_DEPARTMENT_WHERE_ID = "select * from DEPARTMENT where ID = ";
    private static final String FROM_DEPARTMENT = "select * from DEPARTMENT";
    private static final String ID = "ID";
    private static final String NAME = "NAME";
    private static final String LOCATION = "LOCATION";

    @Override
    public Optional<Department> getById(BigInteger Id) {
        return Optional.ofNullable(getDepartment(FROM_DEPARTMENT_WHERE_ID + Id));
    }

    @Override
    public List<Department> getAll() {
        List<Department> departments = new ArrayList<>();
        try {
            Connection connection = ConnectionSource.instance().createConnection();
            Statement statement = connection.createStatement();
            ResultSet rs = statement.executeQuery(FROM_DEPARTMENT);
            while (rs.next()) {
                departments.add(new Department(
                        new BigInteger(String.valueOf(rs.getString(ID))),
                        rs.getString(NAME),
                        rs.getString(LOCATION))
                );
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return departments;
    }

    @Override
    public Department save(Department department) {
        try {
            Connection connection = ConnectionSource.instance().createConnection();
            Statement statement = connection.createStatement();
            if (isNonExistId(department)) {
                statement.executeUpdate(buildInsert(
                        department.getId().toString(),
                        department.getName(),
                        department.getLocation())
                );
            } else {
                statement.executeUpdate(buildUpdate(
                        department.getId().toString(),
                        department.getName(),
                        department.getLocation())
                );
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return getDepartment(FROM_DEPARTMENT_WHERE_ID + department.getId());
    }

    @Override
    public void delete(Department department) {
        try {
            Connection connection = ConnectionSource.instance().createConnection();
            Statement statement = connection.createStatement();
            statement.executeUpdate(buildDelete(department.getId().toString()));
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private Department getDepartment(String query) {
        Department department = null;
        try {
            Connection connection = ConnectionSource.instance().createConnection();
            Statement statement = connection.createStatement();
            ResultSet rs = statement.executeQuery(query);
            while (rs.next()) {
                department = new Department(
                        new BigInteger(String.valueOf(rs.getString(ID))),
                        rs.getString(NAME),
                        rs.getString(LOCATION)
                );
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return department;
    }

    private boolean isNonExistId(Department department) {
        return getAll().stream().noneMatch(dep -> dep.getId().equals(department.getId()));
    }

    public String buildInsert(String id, String name, String location) {
        return "insert into DEPARTMENT (ID, NAME, LOCATION) values ('" + id + "', '" + name + "', '" + location + "')";
    }

    public String buildUpdate(String id, String name, String location) {
        return "update DEPARTMENT set NAME = '" + name + "', LOCATION = '" + location + "' where ID = '" + id + "'";
    }

    public String buildDelete(String id) {
        return "delete from DEPARTMENT where ID = '" + id + "'";
    }
}