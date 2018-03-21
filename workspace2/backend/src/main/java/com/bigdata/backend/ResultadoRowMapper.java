package com.bigdata.backend;

import java.sql.ResultSet;
import java.sql.SQLException;

import org.springframework.jdbc.core.RowMapper;

public class ResultadoRowMapper implements RowMapper
{
	public Object mapRow(ResultSet rs, int rowNum) throws SQLException {
		Resultado customer = new Resultado();
		
		customer.setNatureza(rs.getString("NATUREZA"));		

		return customer;
	}	
}
