/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package client;

/**
 *
 * @author sas
 */

import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.Locale;

public final class Timer {
	private static DecimalFormat msFormat;
	private static DecimalFormat secFormat;
	static {
		msFormat = (DecimalFormat) NumberFormat.getInstance(Locale.ENGLISH);
		msFormat.applyLocalizedPattern("#,##0.0 ms");
		secFormat = (DecimalFormat) NumberFormat.getInstance(Locale.ENGLISH);
		secFormat.applyLocalizedPattern("#,##0.0 sec");
	}
	private long t0;
	private long t1;

	public Timer() {
		t1 = -1;
		t0 = System.nanoTime();
	}

	private void stop() {
		if (t1 == -1)
			t1 = System.nanoTime();
	}

	public String msElapsed() {
		stop();
		return msFormat.format((t1 - t0) * 1e-6);
	}

	public String secElapsed() {
		stop();
		return secFormat.format((t1 - t0) * 1e-9);
	}

	public Double msDuration() {
		stop();
		return (t1 - t0) * 1e-6;
	}
}
