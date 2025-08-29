package com.mongodb.oploganalyzer;

public class OplogEntryKey {
    
    public String ns;
    public String op;
    
    public OplogEntryKey(String ns, String op) {
        this.ns = ns;
        this.op = op;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((ns == null) ? 0 : ns.hashCode());
        result = prime * result + ((op == null) ? 0 : op.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        OplogEntryKey other = (OplogEntryKey) obj;
        if (ns == null) {
            if (other.ns != null)
                return false;
        } else if (!ns.equals(other.ns))
            return false;
        if (op == null) {
            if (other.op != null)
                return false;
        } else if (!op.equals(other.op))
            return false;
        return true;
    }
    
    @Override
    public String toString() {
        return ns + "::" + op;
    }

}
