package extraction.output;

import java.util.LinkedHashMap;

import extraction.pattern.Match;
import extraction.pattern.Match.TermType;

public class TermOutputObject implements OutputObject {
	
	public final String type;
	public final int level;
	public final int no;
	public final int position;
	public final boolean combined;
	public final String nouns;
	public final String[] nounTags;
	public final String lemmas;
	public final String premod;
	public final String[] premodTags;
	public final String postmod;
	public final String[] postmodTags;
	
	private final LinkedHashMap<String, String> data;
	
	public TermOutputObject(String type, int level, int no, int position, boolean combined, String nouns, String[] nounTags, String lemmas, String premod, String[] premodTags, String postmod, String[] postmodTags) {
		this.data = new LinkedHashMap<String, String>();
		this.type = type;
		this.data.put("type", this.type);
		this.level = level;
		this.data.put("level", Integer.toString(this.level));
		this.no = no;
		this.data.put("no", Integer.toString(this.no));
		this.position = position;
		this.data.put("position", Integer.toString(this.position));
		this.combined = combined;
		this.data.put("combined", this.combined ? "1" : "0");
		this.nouns = nouns;
		this.data.put("nouns", this.nouns);
		this.nounTags = nounTags;
		this.data.put("nountags", String.join(":", this.nounTags));
		this.lemmas = lemmas;
		this.data.put("lemmas", this.lemmas);
		this.premod = premod;
		this.data.put("premod", this.premod);
		this.premodTags = premodTags;
		this.data.put("premodtags", String.join(":", this.premodTags));
		this.postmod = postmod;
		this.data.put("postmod", this.postmod);
		this.postmodTags = postmodTags;
		this.data.put("postmodtags", String.join(":", this.postmodTags));
	}
	
	public TermOutputObject(Match match) {
		this(match.type == TermType.CLASS ? "C" : "I", match.level, match.no, match.position, match.combined, match.term.nouns, match.term.nounTags, match.term.lemmas, match.term.preMod, match.term.preModTags, match.term.postMod, match.term.postModTags);
	}

	@Override
	public String[] getAttributes() {
		return new String[] {"type", "level", "no", "position", "combined", "nouns", "nountags", "lemmas", "premod", "premodtags", "postmod", "postmodtags"};
	}

	@Override
	public LinkedHashMap<String, String> getData() {
		return this.data;
	}
	
	public static TermOutputObject fromLineParts(String ...parts) {
		return new TermOutputObject(parts[0], Integer.parseInt(parts[1]), Integer.parseInt(parts[2]), Integer.parseInt(parts[3]), parts[4].equals("1"), parts[5], parts[6].split(":"), parts[7], parts[8], parts[9].split(":"), parts[10], parts[11].split(":"));
	}

}
