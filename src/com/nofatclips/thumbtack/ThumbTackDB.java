package com.nofatclips.thumbtack;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.List;

import com.nofatclips.thumbtack.db.DataBase;
import com.nofatclips.thumbtack.db.InvalidRollbackException;

public class ThumbTackDB {

	private DataBase DB;
	
	public ThumbTackDB() {
		this.DB = new DataBase();
	}
	
	public static void main(String[] args) {
		ThumbTackDB instance = new ThumbTackDB();
		try {
			instance.repl();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public void repl() throws IOException {
		String s;
		BufferedReader stdin = new BufferedReader(new InputStreamReader(System.in));
		boolean keepRunning = true;
		while (keepRunning) {
			s = stdin.readLine();
			if (s == null || s.length() == 0) continue;
			String[] items = s.split(" ");
			String command = items[0];
			List<String> arguments = Arrays.asList(items).subList(1, items.length);
			try {
				keepRunning = execute(command, arguments);				
			} catch (IndexOutOfBoundsException e){
				System.out.println("INVALID ARGUMENTS FOR " + command);
			}
		}

	}

	// TODO Refactor this using Command pattern
	private boolean execute(String command, List<String> arguments) throws IndexOutOfBoundsException {
		command = command.toUpperCase();
		if (command.equals("SET")) {
			this.DB.set(arguments.get(0), arguments.get(1));
		} else if (command.equals("GET")) {
			String s = this.DB.get(arguments.get(0));
			System.out.println((s == null) ? "NULL" : s);
		} else if (command.equals("UNSET")) {
			this.DB.unset(arguments.get(0));
		} else if (command.equals("NUMEQUALTO") || command.equals("NET")) {
			System.out.println(this.DB.numEqualTo(arguments.get(0)));;
		} else if (command.equals("BEGIN")) {
			this.DB.begin();
		} else if (command.equals("ROLLBACK")) {
			try {
				this.DB.rollback();
			} catch (InvalidRollbackException e) {
				System.out.println("INVALID ROLLBACK");
			}
		} else if (command.equals("COMMIT")) {
			this.DB.commit();
		} else if (command.equals("END")) {
			return false;
		} else {
			System.out.println("INVALID COMMAND");
		}
		return true;
	}

}
