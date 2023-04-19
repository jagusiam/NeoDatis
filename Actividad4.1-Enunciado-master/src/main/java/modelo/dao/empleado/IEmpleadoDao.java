/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Interface.java to edit this template
 */
package modelo.dao.empleado;


import java.util.Date;
import java.util.List;

import modelo.Empleado;
import modelo.dao.IGenericDao;

/**
 *
 * @author mfernandez
 */
public interface IEmpleadoDao extends IGenericDao<Empleado>{
    
	List<Empleado> findByJob(String job);
	
	boolean exists(Integer empno);
	
	float findAvgSalary(); 
	
	List<Empleado> findEmployeesByHireDate(Date from, Date to);
	
}
