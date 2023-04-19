package modelo.servicio.empleado;


import java.util.List;

import modelo.Empleado;
import modelo.exceptions.DuplicateInstanceException;
import modelo.exceptions.InstanceNotFoundException;

public interface IServicioEmpleado {

	public long createEmpleado(Empleado empleado) throws DuplicateInstanceException;
	
	public boolean deleteEmpleado(Empleado empleado);
	public boolean updateEmpleado(Empleado empleado);
	public Empleado findByOID(long longOid) throws InstanceNotFoundException ;
	
	public List<Empleado> findAll();

	public List<Empleado> findByJob(String string);
	

	float findAvgSalary();
	
	List<Empleado> findEmployeesByHireDate(int yearFrom, int monthFrom, int dayFrom, int yearTo, int monthTo, int dayTo);
	
	
}


