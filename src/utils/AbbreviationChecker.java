package utils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import edu.stanford.nlp.ie.machinereading.domains.ace.reader.RobustTokenizer.AbbreviationMap;

public class AbbreviationChecker {
	
	public final static String[] ABBREVIATIONS = { "A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K", "L", "M", "N",
			"O", "P", "Q", "R", "S", "T", "U", "V", "W", "X", "Y", "Z", "Adj", "Adm", "Adv", "Asst", "Bart", "Bldg",
			"Brig", "Bros", "Capt", "Cmdr", "Col", "Comdr", "Con", "Corp", "Cpl", "DR", "Dr", "Drs", "Ens", "Gen",
			"Gov", "Hon", "Hr", "Hosp", "Insp", "Lt", "MM", "MR", "MRS", "MS", "Maj", "Messrs", "Mlle", "Mme", "Mr",
			"Mrs", "Ms", "Msgr", "Op", "Ord", "Pfc", "Ph", "Prof", "Pvt", "Rep", "Reps", "Res", "Rev", "Rt", "Sen",
			"Sens", "Sfc", "Sgt", "Sr", "St", "Supt", "Surg", "v", "vs", "i.e", "rev", "e.g", "No", "Nos", "Art", "Nr",
			"pp" };

	private AbbreviationMap am = new AbbreviationMap(true); //case-insensitive;
	private Set<String> abbrSet = new HashSet<String>();
	
	private static AbbreviationChecker instance;
	
	public AbbreviationChecker() {
		abbrSet.addAll(toLowerCase(Arrays.asList(ABBREVIATIONS)));
	}
	
	public boolean isAbbr(String word) {
		if (word.endsWith(".")) {
			word = word.substring(0, word.length() - 1);
		}
		return abbrSet.contains(word) || am.contains(word + ".");
	}
	
	private static List<String> toLowerCase(List<String> words) {
		List<String> normWords = new ArrayList<>();
		for(String word: words) normWords.add(word.toLowerCase());
		return normWords;
    }
	
	public static AbbreviationChecker getInstance() {
		if (AbbreviationChecker.instance == null) {
			AbbreviationChecker.instance = new AbbreviationChecker();
		}
		return AbbreviationChecker.instance;
	}
	
}
