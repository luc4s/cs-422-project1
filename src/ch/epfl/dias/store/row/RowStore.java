package ch.epfl.dias.store.row;

import java.io.BufferedReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;

import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.Store;

public class RowStore extends Store {

	private DataType[] mSchema;
	private Path mFilePath;
	private String mDelimiter;
	
	private ArrayList<DBTuple> mData;

	public RowStore(DataType[] schema, String fileName, String delimiter) {
		mFilePath = Paths.get(fileName);
		mSchema = schema;
		mDelimiter = delimiter;
		mData = new ArrayList<>();
	}

	@Override
	public void load() {
		try {
			BufferedReader reader = Files.newBufferedReader(mFilePath);
			
			String line;
			while ((line = reader.readLine()) != null)  {
				String[] tokens = line.split(mDelimiter);
				Object[] fields = new Object[tokens.length];
				for (int i = 0; i < tokens.length; ++i) {
					if (tokens[i].equals("")) {
						fields[i] = null;
						continue;
					}
					switch (mSchema[i]) {
						case INT:
							fields[i] = Integer.valueOf(tokens[i]);
							break;
						case DOUBLE:
							fields[i] = Double.valueOf(tokens[i]);
							break;
						case BOOLEAN:
							fields[i] = Boolean.valueOf(tokens[i]);
							break;
						case STRING:
							fields[i] = tokens[i];
							break;
						default:
							throw new RuntimeException("Unrecognized type.");
					}
				}
				mData.add(new DBTuple(fields, mSchema));
			}
			reader.close();
			mData.add(new DBTuple());
		}
		catch(Exception e) {
			System.err.println("Error occured when loading file '" + mFilePath + "'");
			e.printStackTrace();
		}
	}

	@Override
	public DBTuple getRow(int rowNumber) {
		if (rowNumber >= mData.size())
			return mData.get(mData.size() - 1);

		return mData.get(rowNumber);
	}
}
