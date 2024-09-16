/*-----------------------------------------------------------------------------
 Copyright © 2024, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
 SPDX-License-Identifier: Apache-2.0
-----------------------------------------------------------------------------*/

import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URLConnection;

import javax.xml.bind.DatatypeConverter;

public class DownloadFile {
	public static void main(String[] args) throws Exception {
		// Check for input arguments
		if (args.length < 4) {
			System.out.println("Usage java DownloadFile <user> <password> <url> <target file>");
			return;
		}
		String user = args[0];
		String pass = args[1];
		String url = args[2];
		File outputFile = new File(args[3]);

		String userpass = user + ":" + pass;
		String basic = "Basic " + DatatypeConverter.printBase64Binary(userpass.getBytes());
		System.setProperty("java.net.preferIPv4Stack", "true");
		URLConnection connection = new URI(url).toURL().openConnection();
		connection.setRequestProperty("Authorization", basic);

		byte[] buffer = new byte[8192];
		InputStream is = connection.getInputStream();
		OutputStream os = new FileOutputStream(outputFile);
		for (int n = is.read(buffer); n > 0; n = is.read(buffer))
			os.write(buffer, 0, n);
		is.close();
		os.close();
	}
}
