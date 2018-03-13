package ch.epfl.dias.store.column;

import java.io.BufferedReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;

import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.Store;

public class ColumnStore extends Store {

	private Path mFilePath;
	private String mDelimiter;
	private DataType[] mSchema;

	public DBColumn[] mData;

	public ColumnStore(DataType[] schema, String fileName, String delimiter) {
		mFilePath = Paths.get(fileName);
		mDelimiter = delimiter;
		mData = new DBColumn[schema.length];
		mSchema = schema;
	}
	
	public int columnsCount() {
		return mData.length;
	}

	@Override
	public void load() {
		try {
			BufferedReader reader = Files.newBufferedReader(mFilePath);
			
			ArrayList< ArrayList<Object> > columns = new ArrayList<>(mData.length);
			for (int i = 0; i < mData.length; ++i)
				columns.add(new ArrayList<>());

			String line;
			while ((line = reader.readLine()) != null)  {
				String[] tokens = line.split(mDelimiter);
				for (int i = 0; i < tokens.length; ++i) {
					if (tokens[i].equals("")) {
						columns.get(i).add(null);
						continue;
					}
					switch (mSchema[i]) {
						case INT:
							columns.get(i).add(Integer.valueOf(tokens[i]));
							break;
						case DOUBLE:
							columns.get(i).add(Double.valueOf(tokens[i]));
							break;
						case BOOLEAN:
							columns.get(i).add(Boolean.valueOf(tokens[i]));
							break;
						case STRING:
							columns.get(i).add(tokens[i]);
							break;
						default:
							throw new RuntimeException("Unrecognized type.");
					}
				}
			}
			reader.close();
			for (int i = 0; i < columns.size(); ++i)
				mData[i] = new DBColumn(columns.get(i).toArray(), mSchema[i]);
		}
		catch(Exception e) {
			System.err.println("Error occured when loading file '" + mFilePath + "'.");
			e.printStackTrace();
		}
	}

	@Override
	public DBColumn[] getColumns(int[] columnsToGet) {
		if (columnsToGet == null)
			throw new NullPointerException();
		
		DBColumn[] columns = new DBColumn[columnsToGet.length];
		for (int i = 0; i < columns.length; ++i)
			columns[i] = mData[columnsToGet[i]];
		
		return columns;
	}
}
