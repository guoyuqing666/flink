package flink.dto;


public class Behavior{
    private String user_id;
    private String item_id;
    private String category_id;
    private String behavior;
    private String ts;
	public String getUser_id() {
		return user_id;
	}
	public void setUser_id(String user_id) {
		this.user_id = user_id;
	}
	public String getItem_id() {
		return item_id;
	}
	public void setItem_id(String item_id) {
		this.item_id = item_id;
	}
	public String getCategory_id() {
		return category_id;
	}
	public void setCategory_id(String category_id) {
		this.category_id = category_id;
	}
	public String getBehavior() {
		return behavior;
	}
	public void setBehavior(String behavior) {
		this.behavior = behavior;
	}
	public String getTs() {
		return ts;
	}
	public void setTs(String ts) {
		this.ts = ts;
	}
	public Behavior(String user_id, String item_id, String category_id, String behavior, String ts) {
		super();
		this.user_id = user_id;
		this.item_id = item_id;
		this.category_id = category_id;
		this.behavior = behavior;
		this.ts = ts;
	}
	@Override
	public String toString() {
		return "Behavior [user_id=" + user_id + ", item_id=" + item_id + ", category_id=" + category_id + ", behavior="
				+ behavior + ", ts=" + ts + "]";
	}
    
    
}

