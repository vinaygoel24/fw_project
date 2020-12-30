package freshworks;

import java.io.*;
import org.json.*;
import java.io.IOException;
import java.lang.instrument.Instrumentation;
import java.io.FileNotFoundException;
import java.time.LocalTime;


// To create a key-value data store

public class DataStore {
	private final String Path; 
	private static Instrumentation in;
	DataStore() throws JSONException {								//Initialized using default file path
        Path = "C://Users//Data//Astik.JSON";
        JSONObject jObject = new JSONObject();
        jObject.put(" ", " ");     
         try (PrintWriter pw = new PrintWriter(Path))
        		{
        	      pw.write(jObject.toString());   
        	    }
         catch (IOException io)
         {
        	System.out.println("IOException have been Caught"); 
         }
	}
	DataStore(String path) throws JSONException					//Initialized using given file path
    {     	
        Path = path;
        JSONObject jObject = new JSONObject();
        jObject.put(" ", " ");      
        try (PrintWriter pw = new PrintWriter(path))
        		{
        	      pw.write(jObject.toString());
        	    }
         catch (IOException io)
         {
        	System.out.println("IOException have been Caught"); 
         }
        
        
    }
    
        
    
       
    
    
    // CREATE operation without Time-to-Live property using only Key and Data value pair
    
    
    public  synchronized void Create(String key, JSONObject value) throws Exception  					// ensuring thread safety
    {
    	
    	try 
    	{
    	 if((in.getObjectSize((Object)value)/1024)>16) 		// To check if the size of JSONObject is not more than 16 KB
       	 throw new ValueSizeReached();
    	 else 
    		 if(key.length()>32)
    		 throw new KeySizeReached();
        }
        catch (ValueSizeReached ex) {
         	System.out.println("Value size exceeds maximum limit");
         } catch (KeySizeReached ex) {
         	System.out.println("Enter a Valid Key");
         }
        try (FileReader read = new FileReader(Path)) 
        {
            
            
        	JSONTokener tokenValue = new JSONTokener(read);
            
        	JSONObject tObject = new JSONObject(tokenValue);
        	if (tObject.has(key)) 					//To check if key already exists
                throw new DuplicateKeyFound();
        	else
        	{
        	JSONArray tempArray = new JSONArray();
        	tempArray.put(value); 
            tempArray.put(Integer.MAX_VALUE); 

            LocalTime t = LocalTime.now();
            int TimeValue = t.toSecondOfDay();
            tempArray.put(TimeValue); 
            tObject.put(key, tempArray);
            try (PrintWriter pw = new PrintWriter(Path)) // entry to the file is done
            {
                pw.write(tObject.toString());
                
            } catch (IOException e) {
                System.out.println("IO Exception found");
            }
        	}

        }catch (FileNotFoundException e) {
            System.out.println("File isn't present");
        } catch (DuplicateKeyFound e) {
            System.out.println("Duplicate key found");
        } catch (IOException e) {
            System.out.println("IO Exception found");
        }
        catch(Exception ex) {
        	System.out.println("Exception found");
        }
    }   
    
    // READ operations for JSON Object
    public synchronized JSONObject Read(String key) throws Exception			// ensuring thread safety
    {
        
        try (FileReader read = new FileReader(Path)) 
        {
           
            
        	JSONTokener tokenValue = new JSONTokener(read);
            JSONObject tempValue = new JSONObject(tokenValue);
            if (tempValue.has(key)) 										// To check if key exists
            {
                JSONArray tempArray = new JSONArray();
                tempArray = tempValue.getJSONArray(key);
                LocalTime t = LocalTime.now();
                int CurrTime = t.toSecondOfDay();
                if ((CurrTime - tempArray.getInt(2)) < tempArray.getInt(1)) //To check key lifetime
                    return tempArray.getJSONObject(0);
                else
                    throw new TimeExceeded();

            } else
                throw new WrongKey();

        } 
        catch (WrongKey e) {
            System.out.println("Wrong Key entered");
        } 
        catch (TimeExceeded e) 
        {
            System.out.println("Key lifetime exceeeded");
        } 
        catch (FileNotFoundException e) {
            System.out.println("File doen't exists");
        } 
        catch (IOException e) {
            System.out.println("IO Exception found");
        }
        catch(Exception ex) {
            	System.out.println("Exception found");
            }
        
		return null;
    }
    
    
    // Delete operation 
    
    public synchronized void Delete(String key) throws Exception 					// ensuring thread safety
    {
       
        try (FileReader read = new FileReader(Path))
        {
        	JSONTokener tk= new JSONTokener(read);
            JSONObject tempValue = new JSONObject(tk);
            if (tempValue.has(key)) 			//checking if key exists
            {
                JSONArray tempArray = new JSONArray();
                tempArray = tempValue.getJSONArray(key);
                LocalTime t = LocalTime.now();
                int currTime = t.toSecondOfDay();
                if ((currTime - tempArray.getInt(2)) < tempArray.getInt(1)) //To check the life time condition of key and removes Key,Value
                    tempValue.remove(key);
                else
                    throw new TimeExceeded();
}
            else
                    throw new WrongKey();

         } 
         catch (WrongKey ex) {
             System.out.println("Wrong Key entered");
            } 
         catch (IOException ex) {
                System.out.println("IO Exception found");
            }

         catch (TimeExceeded ex) {
            System.out.println("Key LifeTime Exceeded");
        }
        catch(Exception ex) {
        	System.out.println("Exception found");
        }
    
    }
    
}
