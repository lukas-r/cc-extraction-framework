package extraction.pattern;

import java.util.ArrayList;
import java.util.List;

import extraction.pattern.Pattern.InstanceCount;
import extraction.pattern.Pattern.Numerus;
import extraction.pattern.Pattern.PatternOrder;
import extraction.pattern.Pattern.PatternType;
import extraction.pattern.PatternHelper.POSTag;

public abstract class StandardPatterns {
	
	public final static Pattern PATTERN_SIC_IS_A = Pattern.splitInstanceClass("p_SIC_is_a", "is /an?/", "is an?", Numerus.SINGULAR, Numerus.SINGULAR, InstanceCount.SINGLE);
	public final static Pattern PATTERN_SIC_AND_OTHER = Pattern.splitInstanceClass("p_SIC_and_other", "and other", Numerus.PLURAL, Numerus.UNDEFINED, InstanceCount.MULTIPLE);
	public final static Pattern PATTERN_SIC_AND_MANY_OTHER = Pattern.splitInstanceClass("p_SIC_and_many_other", "and many other", Numerus.PLURAL, Numerus.UNDEFINED, InstanceCount.MULTIPLE);
	public final static Pattern PATTERN_SIC_WAS_A = Pattern.splitInstanceClass("p_SIC_was_a", "was /an?/", "was an?", Numerus.SINGULAR, Numerus.SINGULAR, InstanceCount.SINGLE);
	public final static Pattern PATTERN_SIC_OR_OTHER = Pattern.splitInstanceClass("p_SIC_or_other", "or other", Numerus.PLURAL, Numerus.UNDEFINED, InstanceCount.MULTIPLE);
	public final static Pattern PATTERN_SIC_OR_ANY_OTHER = Pattern.splitInstanceClass("p_SIC_or_any_other", "or any other", Numerus.PLURAL, Numerus.UNDEFINED, InstanceCount.MULTIPLE);
	public final static Pattern PATTERN_SIC_OR_MANY_OTHER = Pattern.splitInstanceClass("p_SIC_or_many_other", "or many other", Numerus.PLURAL, Numerus.UNDEFINED, InstanceCount.MULTIPLE);
	public final static Pattern PATTERN_SIC_OR_SOME_OTHER = Pattern.splitInstanceClass("p_SIC_or_some_other", "or some other", Numerus.PLURAL, Numerus.UNDEFINED, InstanceCount.MULTIPLE);
	public final static Pattern PATTERN_SIC_ARE_A = Pattern.splitInstanceClass("p_SIC_are_a", "are /an?/", "are an?", Numerus.SINGULAR, Numerus.PLURAL, InstanceCount.MULTIPLE);
	public final static Pattern PATTERN_SIC_IS_THE = Pattern.splitInstanceClass("p_SIC_is_the", "is the"/* " + PatternHelper.buildTerm(PatternHelper.fromTag(POSTag.JJS)), "is the \\w+st"*/, Numerus.SINGULAR, Numerus.SINGULAR, InstanceCount.SINGLE);
	//public final static Pattern PATTERN_SIC_IS_THE_MOST = Pattern.splitInstanceClass("p_SIC_is_the_most", "is the most", Numerus.SINGULAR, Numerus.SINGULAR, InstanceCount.SINGLE);
	//public final static Pattern PATTERN_SIC_SORT_OF = Pattern.splitInstanceClass("p_SIC_sort_of", "sort of", Numerus.UNDEFINED, Numerus.UNDEFINED, InstanceCount.MULTIPLE);//bad performance
	
	public final static Pattern PATTERN_SIC_ONE_OF_THE = Pattern.splitInstanceClass("p_SIC_one_of_the", "one of the", "-, one of the", Numerus.PLURAL, Numerus.SINGULAR, InstanceCount.SINGLE);
	//public final static Pattern PATTERN_SIC_WERE_A = Pattern.splitInstanceClass("p_SIC_were_a", "were /an?/", "were an?", Numerus.SINGULAR, Numerus.UNDEFINED, InstanceCount.MULTIPLE);//bad performance
	public final static Pattern PATTERN_SIC_AND_ANY_OTHER = Pattern.splitInstanceClass("p_SIC_and_any_other", "and any other", Numerus.SINGULAR, Numerus.UNDEFINED, InstanceCount.MULTIPLE);
	public final static Pattern PATTERN_SIC_ARE_EXAMPLES_OF = Pattern.splitInstanceClass("p_SIC_are_examples_of", "are examples of", Numerus.PLURAL, Numerus.UNDEFINED, InstanceCount.MULTIPLE);
	public final static Pattern PATTERN_SIC_WERE_EXAMPLES_OF = Pattern.splitInstanceClass("p_SIC_were_examples_of", "were examples of", Numerus.PLURAL, Numerus.UNDEFINED, InstanceCount.MULTIPLE);
	public final static Pattern PATTERN_SIC_ARE_THE = Pattern.splitInstanceClass("p_SIC_are_the", "are the", Numerus.PLURAL, Numerus.UNDEFINED, InstanceCount.MULTIPLE);
	public final static Pattern PATTERN_SIC_COMPARED_TO_OTHER = Pattern.splitInstanceClass("p_SIC_compared_to_other", "compared to other", Numerus.PLURAL, Numerus.UNDEFINED, InstanceCount.MULTIPLE);//bad performance
	//public final static Pattern PATTERN_SIC_COMPARED_TO_ANY_OTHER = Pattern.splitInstanceClass("p_SIC_compared_to_any_other", "compared to any other", Numerus.PLURAL, Numerus.UNDEFINED, InstanceCount.MULTIPLE);//bad performance
	public final static Pattern PATTERN_SIC_COMPARED_TO_MANY_OTHER = Pattern.splitInstanceClass("p_SIC_compared_to_many_other", "compared to many other", Numerus.PLURAL, Numerus.UNDEFINED, InstanceCount.MULTIPLE);//bad performance
	//public final static Pattern PATTERN_SIC_COMPARED_TO_SOME_OTHER = Pattern.splitInstanceClass("p_SIC_compared_to_some_other", "compared to some other", Numerus.PLURAL, Numerus.UNDEFINED, InstanceCount.MULTIPLE);//bad performance
	public final static Pattern PATTERN_SIC_LIKE_OTHER = Pattern.splitInstanceClass("p_SIC_like_other", "like other", Numerus.PLURAL, Numerus.UNDEFINED, InstanceCount.MULTIPLE);
	//public final static Pattern PATTERN_SIC_LIKE_ANY_OTHER = Pattern.splitInstanceClass("p_SIC_like_any_other", "like any other", Numerus.PLURAL, Numerus.UNDEFINED, InstanceCount.MULTIPLE);//bad performance
	//public final static Pattern PATTERN_SIC_LIKE_MANY_OTHER = Pattern.splitInstanceClass("p_SIC_like_many_other", "like many other", Numerus.PLURAL, Numerus.UNDEFINED, InstanceCount.MULTIPLE);//bad performance
	public final static Pattern PATTERN_SIC_LIKE_SOME_OTHER = Pattern.splitInstanceClass("p_SIC_like_some_other", "like some other", Numerus.PLURAL, Numerus.UNDEFINED, InstanceCount.MULTIPLE);
	public final static Pattern PATTERN_SIC_WHICH_IS_A = Pattern.splitInstanceClass("p_SIC_which_is_a", "which is /an?/", "which is an?", Numerus.SINGULAR, Numerus.SINGULAR, InstanceCount.MULTIPLE);
	//public final static Pattern PATTERN_SIC_WHICH_ARE = Pattern.splitInstanceClass("p_SIC_which_are", "which are", Numerus.SINGULAR, Numerus.SINGULAR, InstanceCount.MULTIPLE);//bad performance, sometimes switched
	
	public final static Pattern PATTERN_SIC_AND_SOME_OTHER = Pattern.splitInstanceClass("p_SIC_and_some_other", "and some other", Numerus.PLURAL, Numerus.UNDEFINED, InstanceCount.MULTIPLE);
	public final static Pattern PATTERN_SIC_IS_AN_EXAMPLE_OF = Pattern.splitInstanceClass("p_SIC_is_an_example_of", "is an example of", Numerus.UNDEFINED, Numerus.SINGULAR, InstanceCount.SINGLE);
	public final static Pattern PATTERN_SIC_WAS_AN_EXAMPLE_OF = Pattern.splitInstanceClass("p_SIC_was_an_example_of", "was an example of", Numerus.PLURAL, Numerus.SINGULAR, InstanceCount.SINGLE);
	//public final static Pattern PATTERN_SIC_A_KIND_OF = Pattern.splitInstanceClass("p_SIC_a_kind_of", "a kind of", Numerus.SINGULAR, Numerus.SINGULAR, InstanceCount.SINGLE);//bad performance
	//public final static Pattern PATTERN_SIC_ONE_OF_THOSE = Pattern.splitInstanceClass("p_SIC_one_of_those", "one of those", Numerus.PLURAL, Numerus.SINGULAR, InstanceCount.MULTIPLE);//bad performance
	//public final static Pattern PATTERN_SIC_A_FORM_OF = Pattern.splitInstanceClass("p_SIC_a_form_of", "a form of", Numerus.UNDEFINED, Numerus.UNDEFINED, InstanceCount.MULTIPLE);//bad performance
	//public final static Pattern PATTERN_SIC_ONE_OF_THESE = Pattern.splitInstanceClass("p_SIC_one_of_these", "one of these", Numerus.PLURAL, Numerus.SINGULAR, InstanceCount.MULTIPLE);//bad performance
	//public final static Pattern PATTERN_SIC_FORMS_OF = Pattern.splitInstanceClass("p_SIC_forms_of", "forms of", Numerus.UNDEFINED, Numerus.UNDEFINED, InstanceCount.MULTIPLE);//bad performance
	public final static Pattern PATTERN_SIC_OR_THE_MANY = Pattern.splitInstanceClass("p_SIC_or_the_many", "or the many", Numerus.PLURAL, Numerus.UNDEFINED, InstanceCount.MULTIPLE);//test check numerus
	//public final static Pattern PATTERN_SIC_KINDS_OF = Pattern.splitInstanceClass("p_SIC_kinds_of", "kinds of", Numerus.UNDEFINED, Numerus.UNDEFINED, InstanceCount.MULTIPLE);//bad performance
	public final static Pattern PATTERN_SIC_IS_ONE_OF_THE = Pattern.splitInstanceClass("p_SIC_is_one_of_the", "is one of the", Numerus.PLURAL, Numerus.SINGULAR, InstanceCount.SINGLE);
	public final static Pattern PATTERN_SIC_ARE_SOME_OF_THE = Pattern.splitInstanceClass("p_SIC_are_some_of_the", "are some of the", Numerus.PLURAL, Numerus.UNDEFINED, InstanceCount.MULTIPLE);
	
	
	public final static Pattern PATTERN_SCI_INCLUDING = Pattern.splitClassInstance("p_SCI_including", "including", Numerus.PLURAL, Numerus.UNDEFINED, InstanceCount.MULTIPLE);
	public final static Pattern PATTERN_SCI_SUCH_AS = Pattern.splitClassInstance("p_SCI_such_as", "such as", Numerus.UNDEFINED, Numerus.UNDEFINED, InstanceCount.MULTIPLE);
	public final static Pattern PATTERN_SCI_ESPECIALLY = Pattern.splitClassInstance("p_SCI_especially", "especially", Numerus.UNDEFINED, Numerus.UNDEFINED, InstanceCount.MULTIPLE);
	//public final static Pattern PATTERN_SCI_TYPES = Pattern.splitClassInstance("p_SCI_types", "types", Numerus.UNDEFINED, Numerus.UNDEFINED, InstanceCount.MULTIPLE);//unknown
	public final static Pattern PATTERN_SCI_EXCEPT = Pattern.splitClassInstance("p_SCI_except", "except", Numerus.UNDEFINED, Numerus.UNDEFINED, InstanceCount.MULTIPLE);
	public final static Pattern PATTERN_SCI_PARTICULARLY = Pattern.splitClassInstance("p_SCI_particularly", "particularly", Numerus.UNDEFINED, Numerus.UNDEFINED, InstanceCount.MULTIPLE);
	public final static Pattern PATTERN_SCI_OTHER_THAN = Pattern.splitClassInstance("p_SCI_other_than", "other than", Numerus.UNDEFINED, Numerus.UNDEFINED, InstanceCount.MULTIPLE);
	public final static Pattern PATTERN_SCI_CALLED = Pattern.splitClassInstance("p_SCI_called", "called", Numerus.UNDEFINED, Numerus.SINGULAR, InstanceCount.MULTIPLE);
	
	public final static Pattern PATTERN_SCI_MOSTLY = Pattern.splitClassInstance("p_SCI_mostly", "mostly", Numerus.PLURAL, Numerus.UNDEFINED, InstanceCount.MULTIPLE);
	public final static Pattern PATTERN_SCI_MAINLY = Pattern.splitClassInstance("p_SCI_mainly", "mainly", Numerus.PLURAL, Numerus.UNDEFINED, InstanceCount.MULTIPLE);
	public final static Pattern PATTERN_SCI_EG = Pattern.splitClassInstance("p_SCI_eg", "/e.g./", "e\\.g\\.", Numerus.UNDEFINED, Numerus.UNDEFINED, InstanceCount.MULTIPLE);
	public final static Pattern PATTERN_SCI_IE = Pattern.splitClassInstance("p_SCI_ie", "/i.e./", "i\\.e\\.", Numerus.UNDEFINED, Numerus.UNDEFINED, InstanceCount.MULTIPLE);
	public final static Pattern PATTERN_SCI_FOR_EXAMPLE = Pattern.splitClassInstance("p_SCI_for_example", "for example", Numerus.UNDEFINED, Numerus.UNDEFINED, InstanceCount.MULTIPLE);
	public final static Pattern PATTERN_SCI_IN_PARTICULAR = Pattern.splitClassInstance("p_SCI_in_particular", "in particular", Numerus.UNDEFINED, Numerus.UNDEFINED, InstanceCount.MULTIPLE);
	public final static Pattern PATTERN_SCI_NOTABLY = Pattern.splitClassInstance("p_SCI_notably", "notably", Numerus.UNDEFINED, Numerus.UNDEFINED, InstanceCount.MULTIPLE);
	public final static Pattern PATTERN_SCI_AMONG_THEM = Pattern.splitClassInstance("p_SCI_among_them", "among them", Numerus.PLURAL, Numerus.UNDEFINED, InstanceCount.MULTIPLE);
	public final static Pattern PATTERN_SCI_PRINCIPALLY = Pattern.splitClassInstance("p_SCI_principally", "principally", Numerus.PLURAL, Numerus.UNDEFINED, InstanceCount.MULTIPLE);
	
	public final static Pattern PATTERN_SCI_WHICH_IS_CALLED = Pattern.splitClassInstance("p_SCI_which_is_called", "which is called", Numerus.SINGULAR, Numerus.SINGULAR, InstanceCount.MULTIPLE);
	public final static Pattern PATTERN_SCI_WHICH_ARE_CALLED = Pattern.splitClassInstance("p_SCI_which_are_called", "which are called", Numerus.PLURAL, Numerus.SINGULAR, InstanceCount.MULTIPLE);
	public final static Pattern PATTERN_SCI_WHICH_LOOK_LIKE = Pattern.splitClassInstance("p_SCI_which_look_like", "which look like", Numerus.PLURAL, Numerus.UNDEFINED, InstanceCount.MULTIPLE);
	//public final static Pattern PATTERN_SCI_WHICH_IS_SIMILAR_TO = Pattern.splitClassInstance("p_SCI_which_is_similar_to", "which is similar to", Numerus.SINGULAR, Numerus.SINGULAR, InstanceCount.MULTIPLE);//only instance instance pairs
	public final static Pattern PATTERN_SCI_WHICH_SOUNDS_LIKE = Pattern.splitClassInstance("p_SCI_which_sounds_like", "which sounds like", Numerus.SINGULAR, Numerus.SINGULAR, InstanceCount.MULTIPLE);
	public final static Pattern PATTERN_SCI_WHICH_ARE_SIMILAR_TO = Pattern.splitClassInstance("p_SCI_which_are_similar_to", "which are similar to", Numerus.PLURAL, Numerus.UNDEFINED, InstanceCount.MULTIPLE);
	public final static Pattern PATTERN_SCI_WHICH_IS_NAMED = Pattern.splitClassInstance("p_SCI_which_is_named", "which is named", Numerus.SINGULAR, Numerus.SINGULAR, InstanceCount.SINGLE);
	public final static Pattern PATTERN_SCI_WHICH_ARE_NAMED = Pattern.splitClassInstance("p_SCI_which_are_named", "which are named", Numerus.PLURAL, Numerus.SINGULAR, InstanceCount.SINGLE);
	public final static Pattern PATTERN_SCI_EXAMPLE_OF_THIS = Pattern.splitClassInstance("p_SCI_example_of_this_is", "example of this is", Numerus.UNDEFINED, Numerus.SINGULAR, InstanceCount.MULTIPLE);
	public final static Pattern PATTERN_SCI_EXAMPLES_OF_THIS_ARE = Pattern.splitClassInstance("p_SCI_examples_of_this_are", "examples of this are", Numerus.UNDEFINED, Numerus.UNDEFINED, InstanceCount.MULTIPLE);
	
	
	public final static Pattern PATTERN_CCI_IS = Pattern.compact("p_CCI_is", PatternHelper.buildTerm(PatternHelper.fromTag(POSTag.JJS)), "is", "-st\\s.{1,50}\\sis", Numerus.SINGULAR, Numerus.SINGULAR, InstanceCount.SINGLE);
	public final static Pattern PATTERN_CCI_ARE = Pattern.compact("p_CCI_are", PatternHelper.buildTerm(PatternHelper.fromTag(POSTag.JJS)), "are", "-st\\s.{1,50}\\sare", Numerus.PLURAL, Numerus.UNDEFINED, InstanceCount.MULTIPLE);
	public final static Pattern PATTERN_CCI_MOST_IS = Pattern.compact("p_CCI_most_is", "most " + PatternHelper.buildTerm(PatternHelper.fromTag(POSTag.JJ)), "is", "most\\s.{1,50}\\sis", Numerus.SINGULAR, Numerus.SINGULAR, InstanceCount.SINGLE);
	public final static Pattern PATTERN_CCI_MOST_ARE = Pattern.compact("p_CCI_most_are", "most " + PatternHelper.buildTerm(PatternHelper.fromTag(POSTag.JJ)), "is", "most\\s.{1,50}\\sare", Numerus.PLURAL, Numerus.UNDEFINED, InstanceCount.MULTIPLE);
	public final static Pattern PATTERN_CCI_SUCH_AS = Pattern.compact("p_CCI_such_as", "such", "as", Numerus.UNDEFINED, Numerus.UNDEFINED, InstanceCount.MULTIPLE);
	public final static Pattern PATTERN_CCI_EXAMPLE_OF_IS = Pattern.compact("p_CCI_example_of_is", "example of", "is", Numerus.SINGULAR, Numerus.SINGULAR, InstanceCount.MULTIPLE);
	public final static Pattern PATTERN_CCI_EXAMPLE_OF_ARE = Pattern.compact("p_CCI_example_of_are", "example of", "are", Numerus.PLURAL, Numerus.UNDEFINED, InstanceCount.MULTIPLE);
	//public final static Pattern PATTERN_CCI_WHETHER_OR = Pattern.compact("p_CCI_whether_or", "whether", "or", Numerus.UNDEFINED, Numerus.UNDEFINED, InstanceCount.MULTIPLE);//bad performance
	//public final static Pattern PATTERN_CCI_COMPARE_WITH = Pattern.compact("p_CCI_compare_with", "compare", "with", Numerus.PLURAL, Numerus.UNDEFINED, InstanceCount.MULTIPLE);//wrong pairs
	
	
	public final static List<Pattern> LIST;
	
	static {
		LIST = new ArrayList<Pattern>();
		
		
		LIST.add(PATTERN_SIC_IS_A);
		LIST.add(PATTERN_SIC_AND_OTHER);
		LIST.add(PATTERN_SIC_AND_MANY_OTHER);
		LIST.add(PATTERN_SIC_WAS_A);
		LIST.add(PATTERN_SIC_OR_OTHER);
		LIST.add(PATTERN_SIC_OR_ANY_OTHER);
		LIST.add(PATTERN_SIC_OR_MANY_OTHER);
		LIST.add(PATTERN_SIC_OR_SOME_OTHER);
		LIST.add(PATTERN_SIC_ARE_A);
		LIST.add(PATTERN_SIC_IS_THE);
//		LIST.add(PATTERN_SIC_IS_THE_MOST);
		
		LIST.add(PATTERN_SIC_ONE_OF_THE);
//		LIST.add(PATTERN_SIC_WERE_A);
		LIST.add(PATTERN_SIC_AND_ANY_OTHER);
		LIST.add(PATTERN_SIC_ARE_EXAMPLES_OF);
		LIST.add(PATTERN_SIC_WERE_EXAMPLES_OF);
		LIST.add(PATTERN_SIC_ARE_THE);
		LIST.add(PATTERN_SIC_COMPARED_TO_OTHER);
//		LIST.add(PATTERN_SIC_COMPARED_TO_ANY_OTHER);
		LIST.add(PATTERN_SIC_COMPARED_TO_MANY_OTHER);
//		LIST.add(PATTERN_SIC_COMPARED_TO_SOME_OTHER);
		LIST.add(PATTERN_SIC_LIKE_OTHER);
//		LIST.add(PATTERN_SIC_LIKE_ANY_OTHER);
//		LIST.add(PATTERN_SIC_LIKE_MANY_OTHER);
		LIST.add(PATTERN_SIC_LIKE_SOME_OTHER);
		LIST.add(PATTERN_SIC_WHICH_IS_A);
//		LIST.add(PATTERN_SIC_WHICH_ARE);

		LIST.add(PATTERN_SIC_AND_SOME_OTHER);
		LIST.add(PATTERN_SIC_IS_AN_EXAMPLE_OF);
		LIST.add(PATTERN_SIC_WAS_AN_EXAMPLE_OF);
//		LIST.add(PATTERN_SIC_A_KIND_OF);
//		LIST.add(PATTERN_SIC_ONE_OF_THOSE);
//		LIST.add(PATTERN_SIC_A_FORM_OF);
//		LIST.add(PATTERN_SIC_ONE_OF_THESE);
//		LIST.add(PATTERN_SIC_FORMS_OF);
		LIST.add(PATTERN_SIC_OR_THE_MANY);
//		LIST.add(PATTERN_SIC_KINDS_OF);
		LIST.add(PATTERN_SIC_IS_ONE_OF_THE);
		LIST.add(PATTERN_SIC_ARE_SOME_OF_THE);
		
		
		LIST.add(PATTERN_SCI_INCLUDING);
		LIST.add(PATTERN_SCI_SUCH_AS);
		LIST.add(PATTERN_SCI_ESPECIALLY);
//		LIST.add(PATTERN_SCI_TYPES);
		LIST.add(PATTERN_SCI_EXCEPT);
		LIST.add(PATTERN_SCI_PARTICULARLY);
		LIST.add(PATTERN_SCI_OTHER_THAN);
		LIST.add(PATTERN_SCI_CALLED);
		
		LIST.add(PATTERN_SCI_MOSTLY);
		LIST.add(PATTERN_SCI_MAINLY);
		LIST.add(PATTERN_SCI_EG);
		LIST.add(PATTERN_SCI_IE);
		LIST.add(PATTERN_SCI_FOR_EXAMPLE);
		LIST.add(PATTERN_SCI_IN_PARTICULAR);
		LIST.add(PATTERN_SCI_NOTABLY);
		LIST.add(PATTERN_SCI_AMONG_THEM);

		LIST.add(PATTERN_SCI_WHICH_IS_CALLED);
		LIST.add(PATTERN_SCI_WHICH_ARE_CALLED);
//		LIST.add(PATTERN_SCI_WHICH_LOOKS_LIKE);
		LIST.add(PATTERN_SCI_WHICH_LOOK_LIKE);
//		LIST.add(PATTERN_SCI_WHICH_IS_SIMILAR_TO);
		LIST.add(PATTERN_SCI_WHICH_SOUNDS_LIKE);
		LIST.add(PATTERN_SCI_WHICH_ARE_SIMILAR_TO);
		LIST.add(PATTERN_SCI_WHICH_IS_NAMED);
		LIST.add(PATTERN_SCI_WHICH_ARE_NAMED);
		LIST.add(PATTERN_SCI_EXAMPLE_OF_THIS);
		LIST.add(PATTERN_SCI_EXAMPLES_OF_THIS_ARE);
		
		
		LIST.add(PATTERN_CCI_IS);
		LIST.add(PATTERN_CCI_ARE);
		LIST.add(PATTERN_CCI_MOST_IS);
		LIST.add(PATTERN_CCI_MOST_ARE);
		LIST.add(PATTERN_CCI_SUCH_AS);
		LIST.add(PATTERN_CCI_EXAMPLE_OF_IS);
		LIST.add(PATTERN_CCI_EXAMPLE_OF_ARE);
//		LIST.add(PATTERN_CCI_WHETHER_OR);
//		LIST.add(PATTERN_CCI_COMPARE_WITH);
	}
	
	public static void main(String[] args) {
		int i = 0;
		String npInstance = "$N\\!P_{I}$";
		String npClass = "$N\\!P_{C}$";
		for (Pattern p: LIST) {
			String npLeft, npRight;
			if (p.order == PatternOrder.CLASS_INSTANCE) {
				npLeft = npClass;
				npRight = npInstance;
			} else {
				npLeft = npInstance;
				npRight = npClass;
			}
			String description;
			if (p.type == PatternType.SPLIT) {
				String split = p.split.replaceAll(".\\?", "").replaceAll("[^a-zA-Z ]", "");
				description = npLeft + " " + split + " " + npRight;
			} else {
				description = p.before + " " + npLeft + " " + p.between + " " + npRight;	
			}
			System.out.println(++i + " & " + description + " & " + p.type.name() + " & " + p.order.name().toLowerCase().replace("_", "\\_") + " & 0 & 0,0\\% \\\\ \\hline");
		}
	}

}
