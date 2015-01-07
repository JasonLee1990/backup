package com.happyfi.backup.task;

import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Properties;

import org.quartz.Job;
import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

import com.happyfi.backup.main.Startup;

@JobConfig(identityName = "BackupJob")
public class BackupJob implements Job {

	public void execute(JobExecutionContext ctx) throws JobExecutionException {
		JobDataMap jobMap = ctx.getMergedJobDataMap();
		System.out.println("sourceDir:" + jobMap.getString("sourceDir"));
		String sourceDir = jobMap.getString("sourceDir");
		String destinationDir = jobMap.getString("destinationDir");
		String securityKey = jobMap.getString("securityKey");
		Date updateTime = (Date) jobMap.get("updateTime");
		File dir = new File(sourceDir);
		if (dir.isFile())
			throw new RuntimeException("该路径非目录，请重新设置！");
		File tempDir = new File(jobMap.getString("tempDir"));
		if (!tempDir.exists())
			tempDir.mkdir();
		if (tempDir.listFiles().length > 0)
			FileUtils.clearFolder(tempDir);

		for (File file : dir.listFiles()) {
			Date fileDate = new Date(file.lastModified());
			if (fileDate.after(updateTime) && file.isFile()) {
				FileUtils.copy(file.getPath(), tempDir.getPath());
			}
		}

		try {
			String zipFilePath = ZipUtils.compress(tempDir);
			SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
			ZipUtils.encrypt(zipFilePath, securityKey, destinationDir, sdf.format(new Date(System.currentTimeMillis())) + ZipUtils.EXT);
			new File(zipFilePath).delete();
			Properties prop = new Properties();
			InputStream stream = Class.class.getResourceAsStream(Startup.CONFIG_PATH);
			prop.load(stream);
			sdf.applyPattern("yyyy-MM-dd HH:mm:ss");
			Date time = sdf.parse(prop.getProperty("updateTime"));
			Calendar calendar = Calendar.getInstance();
			calendar.setTime(time);
			calendar.add(Calendar.DAY_OF_MONTH, 1);
			prop.setProperty("updateTime", sdf.format(calendar.getTime()));
			FileOutputStream fos = new FileOutputStream(Startup.CONFIG_PATH); 
			// 将Properties集合保存到流中 
			prop.store(fos, "update"); 
			stream.close();
			fos.close();// 关闭流 
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

}
