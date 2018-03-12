public class IndPair {
    private final int row;
    private final int col;
    public IndPair(int inrow, int incol)
    {
        row = inrow;
        col = incol;
    }
    public int rerow()   { return row; }
    public int recol() { return col; }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        IndPair indPair = (IndPair) o;

        if (row != indPair.row) return false;
        return col == indPair.col;
    }

    @Override
    public int hashCode() {
        int result = row;
        result = 31 * result + col;
        return result;
    }
}
