package modelo.servicio.empleado;

import java.util.Date;
import java.util.List;

import modelo.Empleado;
import modelo.dao.empleado.EmpleadoNeoDatisDao;
import modelo.dao.empleado.IEmpleadoDao;
import modelo.exceptions.DuplicateInstanceException;
import modelo.exceptions.InstanceNotFoundException;
import util.Utils;

public class ServicioEmpleado implements IServicioEmpleado {
	
	private IEmpleadoDao empleadoDao;
	
	public ServicioEmpleado() {
		empleadoDao= new EmpleadoNeoDatisDao();
	}

	@Override
	public long createEmpleado(Empleado empleado) throws DuplicateInstanceException {

			return empleadoDao.create(empleado);

	}

	@Override
	public boolean deleteEmpleado(Empleado empleado) {
	return empleadoDao.delete(empleado);
	}

	@Override
	public boolean updateEmpleado(Empleado empleado) {
		return empleadoDao.update(empleado);
	}

	@Override
	public Empleado findByOID(long longOid) throws InstanceNotFoundException {
		return empleadoDao.read(longOid);
	}

	@Override
	public List<Empleado> findAll() {
		return empleadoDao.findAll();
	}

	@Override
	public List<Empleado> findByJob(String string) {
		return empleadoDao.findByJob(string);
	}

	@Override
	public float findAvgSalary() {
		return empleadoDao.findAvgSalary();
	}

	@Override
	public List<Empleado> findEmployeesByHireDate(int yearFrom, int monthFrom, int dayFrom, int yearTo, int monthTo, int dayTo) {
		Date from = Utils.toDate(yearFrom, monthFrom, dayFrom);
		Date to = Utils.toDate(yearTo, monthTo, dayTo);
		
		return empleadoDao.findEmployeesByHireDate(from, to);
	}
	


	
}
