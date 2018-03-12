package ch.epfl.dias.store.PAX;

import java.io.BufferedReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;

import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.Store;
import ch.epfl.dias.store.column.DBColumn;
import ch.epfl.dias.store.row.DBTuple;

public class PAXStore extends Store {
	private Path mFilePath;
	private String mDelimiter;
	private DataType[] mSchema;
	private final int mTuplesPerPage;

	private DBPAXpage[] mPages;

	public PAXStore(DataType[] schema, String fileName, String delimiter, int tuplesPerPage) {
		mFilePath = Paths.get(fileName);
		mDelimiter = delimiter;
		mPages = null;
		mSchema = schema;
		mTuplesPerPage = tuplesPerPage;
	}

	@Override
	public void load() {
		try {
			BufferedReader reader = Files.newBufferedReader(mFilePath);
			
			ArrayList< ArrayList<Object> > columns = new ArrayList<>(mSchema.length);
			ArrayList<DBPAXpage> pages = new ArrayList<>();

			for (int i = 0; i < mSchema.length; ++i)
				columns.add(new ArrayList<>());

			String line;
			int counter = 0;
			while ((line = reader.readLine()) != null)  {
				String[] tokens = line.split(mDelimiter);
				for (int i = 0; i < tokens.length; ++i) {
					if (tokens[i].equals(""))
						columns.get(i).add(null);
					else {
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
					// Reached tuples limit => create new PAXPage
					if (++counter == mTuplesPerPage) {
						DBColumn[] fields = new DBColumn[mSchema.length];
						for (int j = 0; j < fields.length; ++j) {
							fields[j] = new DBColumn(columns.get(j).toArray());
							columns.get(j).clear();
						}

						pages.add(new DBPAXpage(fields, mSchema));
						counter = 0;
					}
				}
			}
			reader.close();
			mPages = new DBPAXpage[pages.size()];
			for (int i = 0; i < mPages.length; ++i)
				mPages[i] = pages.get(i);
		}
		catch(Exception e) {
			System.err.println("Error occured when loading file '" + mFilePath + "'.");
			e.printStackTrace();
		}
	}

	@Override
	public DBTuple getRow(int rowNumber) {
		final int pageIndex = rowNumber % mTuplesPerPage;
		final int pageRowIndex = rowNumber - (pageIndex * mTuplesPerPage);
		return mPages[pageIndex].getRow(pageRowIndex);
	}
}
