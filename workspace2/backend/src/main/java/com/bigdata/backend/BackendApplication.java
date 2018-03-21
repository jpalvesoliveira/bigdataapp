package com.bigdata.backend;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.bind.annotation.RequestParam;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.spark.launcher.SparkAppHandle;
import org.apache.spark.launcher.SparkAppHandle.State;
import org.apache.spark.launcher.SparkLauncher;

@RestController
@EnableAutoConfiguration
public class BackendApplication {

	private SparkAppHandle handle;
	public Boolean executeCommand(String ano, String mes) {
		try {
		   handle = new SparkLauncher()
			        .setAppResource("/home/bigdata/Documentos/bigdataapp/bigdataapp/target/scala-2.11/bigdataapp_2.11-1.0.jar").addAppArgs(ano,mes)
			        .setMainClass("SpedingWatcherApp")
			        .setMaster("localhost")
			        .setSparkHome("/home/bigdata/spark-2.2.1-bin-hadoop2.7")
			        .setConf(SparkLauncher.DRIVER_MEMORY, "2g")
			        .addSparkArg("--packages", "mysql:mysql-connector-java:6.0.5")
			        .startApplication();
			
			handle.addListener(new SparkAppHandle.Listener() {
		        @Override
		        public void stateChanged(SparkAppHandle handle) {
		            System.out.println(handle.getAppId() + "----" + handle.getState());
		        }

		        @Override
		        public void infoChanged(SparkAppHandle handle) {
		            System.out.println(handle.getAppId() + "----" + handle.getState());
		        }
		    });			

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return Boolean.TRUE;
    }	
	
    @Autowired
    JdbcTemplate jdbcTemplate;
	
	@GetMapping("/consultar")
	public List<Resultado> consultar(@RequestParam(value="ano") String ano, @RequestParam(value="mes") String mes) {
		executeCommand(ano, mes);
		
		while (handle.getState() != State.FINISHED)
		{
			try {
				Thread.sleep(1000L);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				return null;
			}
		}
		
		List<Resultado> resultados = new ArrayList<Resultado>();
		List<Map<String,Object>> rows = jdbcTemplate.queryForList("SELECT NATUREZA, TOTAL FROM bigdata.TOTAIS"); 
		
		for (Map<String,Object> row : rows) {
			Resultado customer = new Resultado();
			
			customer.setNatureza((String)(row.get("NATUREZA")));
			/* customer.setRazaoSocial((String)(row.get("RAZAOSOCIAL")));
			customer.setFatasia((String)(row.get("FANTASIA")));
			customer.setCNAE((String)(row.get("CNAE")));
			customer.setCodigo((Integer) (row.get("CODIGO"))); */
			customer.setTotal((Long)(row.get("TOTAL")));		
			resultados.add(customer);
		}		
				
		return resultados;
	}
	
	public static void main(String[] args) {
		SpringApplication.run(BackendApplication.class, args);
	}
}
