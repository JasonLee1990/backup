package com.happyfi.backup.main;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Properties;

import org.apache.commons.lang3.StringUtils;
import org.quartz.Job;
import org.quartz.JobBuilder;
import org.quartz.JobDetail;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.SchedulerFactory;
import org.quartz.SimpleScheduleBuilder;
import org.quartz.SimpleTrigger;
import org.quartz.TriggerBuilder;
import org.quartz.impl.StdSchedulerFactory;

import com.happyfi.backup.task.JobConfig;

public class Startup {
	
	
	public final static String CONFIG_PATH = "/backupConfig.properties";
	private final static String TASKS_PATH = "/tasks.config";
	//默认配置静态类
	private static class DefaultConfig {
		private int timeSplite = 1;
		private int repeatCount = 10;
		private String sourceDir = "F:/data/";
		private String destinationDir = "G:/backup/";
		private Date updateTime = new Date();
		
		public Date getUpdateTime() {
			return updateTime;
		}
		public int getTimeSplite() {
			return timeSplite;
		}
		public int getRepeatCount() {
			return repeatCount;
		}
		public String getSourceDir() {
			return sourceDir;
		}
		public String getDestinationDir() {
			return destinationDir;
		}
		
    }
	
	public static void main(String[] args) throws ClassNotFoundException, IOException, ParseException {

		// 通过SchedulerFactory来获取一个调度器
		try {
			SchedulerFactory schedulerFactory = new StdSchedulerFactory();
			Scheduler scheduler = schedulerFactory.getScheduler();
			//任务配置加载
			Properties config = loadConfig();
			DefaultConfig dConfig = new DefaultConfig();
			SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			Integer timeSplite = StringUtils.isBlank(config.getProperty("timeSplite")) ? dConfig.getTimeSplite():Integer.valueOf(config.getProperty("timeSplite"));
			Integer repeatCount = StringUtils.isBlank(config.getProperty("repeatCount")) ? dConfig.getRepeatCount():Integer.valueOf(config.getProperty("repeatCount"));
			Date updateTime = StringUtils.isBlank(config.getProperty("updateTime")) ? dConfig.getUpdateTime():sdf.parse(config.getProperty("updateTime"));
			String sourceDir = StringUtils.isBlank(config.getProperty("sourceDir")) ? dConfig.getSourceDir():config.getProperty("sourceDir");
			String destinationDir = StringUtils.isBlank(config.getProperty("destinationDir")) ? dConfig.getDestinationDir():config.getProperty("destinationDir");
			String tempDir = StringUtils.isBlank(config.getProperty("tempDir")) ? dConfig.getDestinationDir():config.getProperty("tempDir");
			String securityKey = config.getProperty("securityKey");
			if(StringUtils.isBlank(securityKey)) return;
			// new一个触发器
			SimpleTrigger trigger = TriggerBuilder
					.newTrigger()
					.withIdentity("myTrigger", "myTriggerGroup")
					.withSchedule(SimpleScheduleBuilder
							.simpleSchedule()
							.withIntervalInMinutes(timeSplite)
							.withRepeatCount(repeatCount))
					.startAt(new Date(System.currentTimeMillis()))
					.build();
			// 引进作业程序
			List<Class<Job>> taskClasses = loadTasks();
			for(Class<Job> taskClass:taskClasses){
				JobConfig jc = taskClass.getAnnotation(JobConfig.class);
				JobBuilder jb = JobBuilder.newJob(taskClass);
				if(jc != null){
					if(!StringUtils.isBlank(jc.identityName()) && !StringUtils.isBlank(jc.identityGroup())){
						jb.withIdentity(jc.identityName(), jc.identityGroup());
					} else if(!StringUtils.isBlank(jc.identityName())) {
						jb.withIdentity(jc.identityName());
					}
					if(!StringUtils.isBlank(jc.description())){
						jb.withDescription(jc.description());
					}
				}
				JobDetail jobDetail = jb.build();
				//向任务传递参数
				jobDetail.getJobDataMap().put("sourceDir", sourceDir);
				jobDetail.getJobDataMap().put("destinationDir", destinationDir);
				jobDetail.getJobDataMap().put("updateTime", updateTime);
				jobDetail.getJobDataMap().put("tempDir", tempDir);
				jobDetail.getJobDataMap().put("securityKey", securityKey);
				
				scheduler.scheduleJob(jobDetail, trigger);
			}
			// 启动调度器
			scheduler.start();
		} catch (SchedulerException e) {
			e.printStackTrace();
		}
	}

	@SuppressWarnings("unchecked")
	private static <T extends Job> List<Class<T>> loadTasks() throws IOException, ClassNotFoundException {
		InputStream config = Startup.class.getResourceAsStream(TASKS_PATH);
		List<Class<T>> list = new ArrayList<Class<T>>();
		if (config.available() > 0) {
			InputStreamReader r = new InputStreamReader(config);
			BufferedReader reader = new BufferedReader(r);
			while (reader.ready()) {
				String line = reader.readLine();
				if (!StringUtils.isBlank(line)) {
					Class<?> cls = Class.forName(line.trim());
					Class<?>[] interfaces = cls.getInterfaces();
					if(interfaces.length > 0){
						for(Class<?> iface:interfaces){
							if(iface.equals(Job.class)){
								list.add((Class<T>) cls);
								break;
							}
						}
					}
				}
			}
			reader.close();
			r.close();
			config.close();
		}
		return list;
	}
	
	private static Properties loadConfig() throws IOException{
		Properties prop = new Properties();
		InputStream stream = Startup.class.getResourceAsStream(CONFIG_PATH);
		prop.load(stream);
		stream.close();
		return prop;
	}

}
