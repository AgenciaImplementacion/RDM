/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package info.proadmintierra.rdm.drivers;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Postgres {

	private Connection c = null;
	private Statement stmt = null;

	public ResultSet query(String sql) {
		ResultSet response = null;
		try {
			this.stmt = this.c.createStatement();
			response = this.stmt.executeQuery(sql);
			response.next();
			return response;
		} catch (SQLException ex) {
			Logger.getLogger(Postgres.class.getName()).log(Level.SEVERE, null, ex);
		}
		return response;
	}

	public String queryToString(String sql) {
		String response = "";
		try {
			this.stmt = this.c.createStatement();
			ResultSet rs = stmt.executeQuery(sql);
			rs.next();
			response = rs.getString(1);
			stmt.close();
			c.commit();
			return response;
		} catch (SQLException ex) {
			Logger.getLogger(Postgres.class.getName()).log(Level.SEVERE, null, ex);
		}
		return response;
	}

	public boolean connect(String connectionString, String connectionUser, String connectionPassword,
			String classForName) {
		this.disconnect();
		try {
			Class.forName(classForName);
			c = DriverManager.getConnection(connectionString, connectionUser, connectionPassword);
			c.setAutoCommit(false);
			return true;
		} catch (Exception ex) {
			Logger.getLogger(Postgres.class.getName()).log(Level.SEVERE, null, ex);
			this.disconnect();
		}
		return false;
	}

	public void disconnect() {
		if (this.c != null) {
			try {
				this.stmt.close();
				this.c.close();
			} catch (SQLException ex) {
				Logger.getLogger(Postgres.class.getName()).log(Level.SEVERE, null, ex);
			}
		}
		this.c = null;
	}

	public ResultSet getResultSetFromSql(String sql) {
		ResultSet response = null;
		try {
			this.stmt = this.c.createStatement();
			response = this.stmt.executeQuery(sql);
			return response;
		} catch (SQLException ex) {
			Logger.getLogger(Postgres.class.getName()).log(Level.SEVERE, null, ex);
		}
		return response;
	}

}