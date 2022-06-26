
import org.json.JSONObject;
import org.json.JSONException;

import org.apache.crunch.DoFn;
import org.apache.crunch.Emitter;
import com.google.common.base.Splitter;

public class GetWords extends DoFn<String, String>{
    private static final Splitter SPLITTER = Splitter.onPattern("\\n").omitEmptyStrings();

    @Override
    public void process(String value, Emitter<String> emitter){
        String cat;
        try {
            for(String tuple:SPLITTER.split(value)) {
                JSONObject obj = new JSONObject(tuple);
                cat = obj.getString("category_name");
                emitter.emit(cat);
            }
        } catch (JSONException e){
            System.out.println("Exception");
            e.printStackTrace();
        }
    }

}
