package com.datatorrent.demos.pi;

import java.beans.Transient;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.common.util.BaseOperator;



public class TempTestOperator extends BaseOperator
{ 
  public transient Trial transientProperty; 
  public Integer restartTimes = 0;
  
  @Override
  public void setup(OperatorContext context) 
  {
    transientProperty = new Trial("restarted " + Integer.toString(restartTimes));
  }
  
  public final transient DefaultInputPort<String> inputPort = new DefaultInputPort<String>()
  {

    @Override
    public void process(String tuple)
    {
      output.emit(tuple + transientProperty.property);
    }
    
  };
  
  public final static transient DefaultOutputPort<String> output = new DefaultOutputPort<String>();
}
