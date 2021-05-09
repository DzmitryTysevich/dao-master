package com.epam.rd.autocode.dao;

import com.epam.rd.autocode.ConnectionSource;
import com.epam.rd.autocode.domain.Department;

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

public class DepartmentDaoImpl implements DepartmentDao {

    private static final String SELECT_QUERY = "select * from DEPARTMENT";
    private static final String ID = "ID";
    private static final String NAME = "NAME";
    private static final String LOCATION = "LOCATION";
    private final Map<BigInteger, Department> departmentsByMap = new HashMap<>();

    @Override
    public Optional<Department> getById(BigInteger Id) {
        putDepartmentsToMap();
        return Optional.ofNullable(departmentsByMap.get(Id));
    }

    @Override
    public List<Department> getAll() {
        putDepartmentsToMap();
        return new ArrayList<>(departmentsByMap.values());
    }

    @Override
    public Department save(Department department) {
        departmentsByMap.put(department.getId(), department);
        return departmentsByMap.get(department.getId());
    }

    @Override
    public void delete(Department department) {
        departmentsByMap.remove(department.getId());
    }

    private void putDepartmentsToMap() {
        try {
            Connection connection = ConnectionSource.instance().createConnection();
            Statement statement = connection.createStatement();
            ResultSet rs = statement.executeQuery(SELECT_QUERY);
            while (rs.next()) {
                departmentsByMap.put(new BigInteger(String.valueOf(rs.getString(ID))),
                        new Department(
                                new BigInteger(String.valueOf(rs.getString(ID))),
                                rs.getString(NAME),
                                rs.getString(LOCATION))
                );
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}