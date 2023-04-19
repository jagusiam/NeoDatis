/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package modelo.dao.empleado;


import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.neodatis.odb.ODB;
import org.neodatis.odb.ODBRuntimeException;
import org.neodatis.odb.OID;
import org.neodatis.odb.ObjectValues;
import org.neodatis.odb.Objects;
import org.neodatis.odb.Values;
import org.neodatis.odb.core.oid.OIDFactory;
import org.neodatis.odb.core.query.IQuery;
import org.neodatis.odb.core.query.IValuesQuery;
import org.neodatis.odb.core.query.criteria.And;
import org.neodatis.odb.core.query.criteria.ICriterion;
import org.neodatis.odb.core.query.criteria.Where;
import org.neodatis.odb.impl.core.query.criteria.CriteriaQuery;
import org.neodatis.odb.impl.core.query.values.ValuesCriteriaQuery;

import modelo.Empleado;
import modelo.dao.AbstractGenericDao;
import modelo.exceptions.InstanceNotFoundException;
import util.ConnectionFactory;
import util.Utils;

/**
 *
 * @author mfernandez
 */
public class EmpleadoNeoDatisDao 
extends AbstractGenericDao<Empleado> 
implements IEmpleadoDao {

	private ODB dataSource;

	public EmpleadoNeoDatisDao() {
		this.dataSource = ConnectionFactory.getConnection();
	}

	@Override
	public long create(Empleado entity) {
		OID oid = null;
		long oidlong =-1;
		try {
			
			oid = this.dataSource.store(entity);
			this.dataSource.commit();

		} catch (Exception ex) {
			
			System.err.println("Ha ocurrido una excepción: " + ex.getMessage());
			this.dataSource.rollback();
			oid = null;
		}
		if(oid!=null) {
			oidlong= oid.getObjectId();
		}
		return oidlong;
	}

	@Override
	public Empleado read(long id) throws InstanceNotFoundException {
		Empleado empleado = null;
		try {
			OID oid = OIDFactory.buildObjectOID(id);
			empleado = (Empleado) this.dataSource.getObjectFromId(oid);
		} catch (ODBRuntimeException ex) {
		
			System.err.println("Ha ocurrido una excepción: " + ex.getMessage());
//Suponemos que no lo encuentra
			throw new InstanceNotFoundException(id, getEntityClass());
		}
		catch(Exception ex) {
			
			System.err.println("Ha ocurrido una excepción: " + ex.getMessage());

		}
		return empleado;
	}

	@Override
	public boolean update(Empleado entity) {
		boolean exito = false;
		try {
			this.dataSource.store(entity);
			this.dataSource.commit();
			exito = true;
		} catch (Exception ex) {			
			System.err.println("Ha ocurrido una excepción: " + ex.getMessage());
			this.dataSource.rollback();
			

		}
		return exito;		
		// nbfs://nbhost/SystemFileSystem/Templates/Classes/Code/GeneratedMethodBody
	}





	

	@Override
	public float findAvgSalary() {
		BigDecimal media =BigDecimal.ZERO;
		ValuesCriteriaQuery valuesCriteriaQuery = new ValuesCriteriaQuery(Empleado.class);
		IValuesQuery ivc = valuesCriteriaQuery.avg("sal");
		Values values = this.dataSource.getValues(ivc);
		while(values.hasNext()) {
			ObjectValues objectValues = (ObjectValues) values.next();			
			media = (BigDecimal) objectValues.getByIndex(0); 
		}
		return media.floatValue();
	}
	

	/**
	 *  Completa la implementación del método public boolean delete(Empleado empleado) 
	 *  para que elimine un Empleado dentro de una transacción y devuelva true si se 
	 *  ha eliminado con éxito y false en caso contrario.
	 */
	@Override
	public boolean delete(Empleado entity) {
		boolean eliminado = false;
		//OID oid = OIDFactory.buildObjectOID(0);
		//Empleado emp = (Empleado)dataSource.getObjectFromId(oid)
				try {
					this.dataSource.delete(entity);
					//this.dataSource.store(entity);
					this.dataSource.commit();
					eliminado = true;
				} catch (Exception ex) {			
					System.err.println("Ha ocurrido un error y no se ha podidio eliminar " + ex.getMessage());
					this.dataSource.rollback();	
				}
				
		// TODO Auto-generated method stub
		return eliminado;
	}
	
	/**
	 *  Completa la implementación del método  public List<Empleado> 
	 *  findAll() para que devuelva una lista de todos los empleados. 
	 *  Puedes ayudarte del método Utils.toList existente en el paquete util.
	 */
	//VERSION 1
//	@Override
//	public List<Empleado> findAll() {
//		List<Empleado> empleadosRecuperados = new ArrayList();
//		Objects<Empleado> objects = dataSource.getObjects(Empleado.class);
//		while (objects.hasNext()) {
//			Empleado empleado = objects.next();
//			empleadosRecuperados.add(empleado);
//			System.out.println("Empleado: " + empleado);
//		}
//		//impresion de comprobacion
//		for (Empleado empleado : empleadosRecuperados) {
//			System.out.println("Empleado " + empleado);
//			
//		}		
//		return empleadosRecuperados;
//	}
	

	
	//VERSION 2: Método findAll() con el uso del metodo toList() de la clase util.Utils
	@Override
	public List<Empleado> findAll() {
		List<Empleado> empleadosRecuperados = new ArrayList();
		Objects<Empleado> objects = dataSource.getObjects(Empleado.class);
		while (objects.hasNext()) {
			Empleado empleado = objects.next();
			empleadosRecuperados = Utils.toList(objects);
			//System.out.println("Empleado: " + empleado);				
		}
		
		return empleadosRecuperados;
	}
	

	//Para que devuelva todos los empleados que tengan el mismo empleo
	@Override
	public List<Empleado> findByJob(String job) {
		
		// TODO Auto-generated method stub
		ICriterion criterio = Where.equal("job", job);
		IQuery query = new CriteriaQuery(Empleado.class, criterio);
		
		//List<Empleado> empleados = (Empleado)dataSource.getObjects(query);
		List<Empleado> empleados = new ArrayList<>();
		
		Objects<Empleado> objects = dataSource.getObjects(query);
		
		while (objects.hasNext()) {
			Empleado empleado = objects.next();
			empleados.add(empleado);
			System.out.println("Empleado con el puesto consultado " + empleado);
		}
		return empleados;
	}

	//para que devuelva true si ya existe un empleado con ese empno
	@Override
	public boolean exists(Integer empno) {
		boolean exist = false;
		OID oid = OIDFactory.buildObjectOID(empno);
		Empleado emp = (Empleado)dataSource.getObjectFromId(oid);
		if (emp != null) {
			exist = true;
			System.out.println("El Empleado con el numero " + empno + " ya existe en la BD.");
		}		
		return exist;
	}

	//para que devuelva todos los empleados contratados entre esas dos fechas (ambas incluidas)
	@Override
	public List<Empleado> findEmployeesByHireDate(Date from, Date to) {
		List<Empleado> empleadosFecha = new ArrayList();
		ICriterion criterio = new And().add(Where.ge("hiredate", from))
				.add(Where.le("hiredate", to));
		IQuery query = new CriteriaQuery(Empleado.class, criterio);
		Objects<Empleado> objects = dataSource.getObjects(query);
		while (objects.hasNext()) {
			Empleado empleado = objects.next();
			empleadosFecha.add(empleado);
			System.out.println("Resultado de Fecha de contratacion: " + empleado);
		}
		// TODO Auto-generated method stub
		return empleadosFecha;
	}
	
	

}
