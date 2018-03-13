package ch.epfl.dias.store;

public enum DataType {
    INT("Integer"), 
    DOUBLE("Double"), 
    BOOLEAN("Boolean"),
    STRING("String");
    
    private String mStrVal;
	
	private DataType(String strVal) {
		mStrVal = strVal;
	}
	
	public String toString() {
		return mStrVal;
	}
}
