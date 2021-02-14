package extraction.pattern;

import java.util.ArrayList;
import java.util.List;

import extraction.output.TermOutputObject;

public class Matches {
	
	public final List<Match> instances;
	public final List<Match> classes;
	public final String part;
	public final int startPosition;
	public final int matchPosition;

	public Matches(List<Match> instances, List<Match> classes, String part, int startPosition, int matchPosition) {
		this.instances = instances;
		this.classes = classes;
		this.part = part;
		this.startPosition = startPosition;
		this.matchPosition = matchPosition;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Matches other = (Matches) obj;
		return this.part != null && other.part != null && this.part.equals(other.part);
	}
	
	public List<TermOutputObject> getTermOutputObjects() {
		List<TermOutputObject> list = new ArrayList<TermOutputObject>();
		for (Match cl: this.classes) {
			list.add(new TermOutputObject(cl));
		}
		for (Match in: this.instances) {
			list.add(new TermOutputObject(in));
		}
		return list;
	}
	
}
