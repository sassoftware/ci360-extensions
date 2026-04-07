/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package client.ci360.tasks;

/**
 *
 * @author sas
 */

import java.time.LocalDateTime;
import java.util.Map;
import java.util.ResourceBundle;
import java.util.concurrent.ConcurrentHashMap;

import org.json.JSONArray;
import org.slf4j.LoggerFactory;

import ch.qos.logback.classic.Logger;
import client.ci360.sql.TaskSql;

public class TaskCache {
	private static final Logger logger = (Logger) LoggerFactory.getLogger(TaskCache.class);
	private static Map<String, Task> tasks = new ConcurrentHashMap<String, Task>();
	private static Map<String, Object> locks = new ConcurrentHashMap<String, Object>();

	public static void init(ResourceBundle config) throws Exception {
		Task.init(config);
		TasksApi.init(config);
	}

	public static Task getTask(String taskVersionId, String taskId) {
		Task task;
		if (taskVersionId == null || taskId == null) {
			logger.warn("Null values not allowed. taskVersionId='{}' taskId='{}'", taskVersionId, taskId);
			return null;
		}
		if (taskVersionId.trim().length() == 0 || taskId.trim().length() == 0) {
			logger.warn("Empty values not allowed. taskVersionId='{}' taskId='{}'", taskVersionId, taskId);
			return null;
		}
		taskVersionId = taskVersionId.trim();
		taskId = taskId.trim();
		LocalDateTime now = LocalDateTime.now();

		/* return task if loaded from UDM by version id, or not expired */
		if ((task = tasks.get(taskVersionId)) != null)
			if (task.getSource() == Source.UDM_BY_VERSION || now.isBefore(task.getExpiresDttm()))
				return task;

		/* Look up task, if expired or not cached */
		synchronized (getLock(taskVersionId)) {
			// is task still missing ?
			if ((task = tasks.get(taskVersionId)) != null)
				if (task.getSource() == Source.UDM_BY_VERSION || now.isBefore(task.getExpiresDttm()))
					return task;

			// Retrieve task from UDM by version id
			if ((task = udmByVersion(taskVersionId)) != null) {
				tasks.put(taskVersionId, task);
				return task;
			}

			// if that fails retrieve task from 360 API task endpoint
			if ((task = taskApiById(taskVersionId, taskId)) != null) {
				tasks.put(taskVersionId, task);
				return task;
			}

			// if all above fails, get task from UDM by task id
			task = udmById(taskId);

			// if that still fails or cached task exists and is more recent
			Task cached = tasks.get(taskId);
			if (useCachedTask(task, cached)) {
				cached.refresh();
				logger.debug("Loaded task from cache: {}", task);
				return cached;
			}

			if (task != null) {
				tasks.put(taskVersionId, task);
				return task;
			}
			logger.error("Unable to load task {}.", taskId);
			return null;
		}
	}

	private static boolean useCachedTask(Task task, Task cached) {
		if (cached == null)
			return false;
		if (task == null)
			return true;
		LocalDateTime cachedDttm = cached.getLastPublishedDttm();
		if (cachedDttm == null)
			return false;
		LocalDateTime taskDttm = task.getLastPublishedDttm();
		if (taskDttm == null)
			return true;
		return cachedDttm.isAfter(taskDttm);
	}

	private static Object getLock(String taskVersionId) {
		Object lock;
		if (null != (lock = locks.get(taskVersionId)))
			return lock;
		synchronized (locks) {
			if (null != (lock = locks.get(taskVersionId)))
				return lock;
			locks.put(taskVersionId, lock = new Object());
			return lock;
		}
	}

	public static boolean hasTask(String taskVersionId) {
		return tasks.containsKey(taskVersionId);
	}

	public static int size() {
		return tasks.size();
	}

	static void putTask(Task task) {
		logger.trace("Adding task {}", task);
		tasks.put(task.getId(), task);
	}

	private static Task udmByVersion(String taskVersionId) {
		Task task = null;
		try {
			task = TaskSql.getTaskByVersion(taskVersionId);
		} catch (Exception e) {
			logger.warn("Failed.", e);
		}
		if (task != null)
			logger.debug("Loaded task from UDM by version: {}", task);
		return task;
	}

	private static Task udmById(String taskId) {
		Task task = null;
		try {
			task = TaskSql.getTaskById(taskId);
		} catch (Exception e) {
			logger.warn("Failed.", e);
		}
		if (task != null)
			logger.debug("Loaded task from UDM by id: {}", task);
		return task;
	}

	private static Task taskApiById(String taskVersionId, String taskId) {
		Task task = null;
		try {
			task = new Task(Source.TASK_API, taskVersionId, TasksApi.getTaskJson(taskId));
		} catch (Exception e) {
			logger.error("Failed", e);
		}
		if (task != null)
			logger.debug("Loaded task from task endpoint: {}", task);
		return task;
	}


}
