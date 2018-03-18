package hpang.kafka.producer;

public interface Producer {

	enum MODE{IGNORE, SYSNCHRONIZE, ASYSNCHRONIZE}
	
	void start(MODE mode);

}