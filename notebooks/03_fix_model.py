"""
Fix Keras Model for Compatibility
Converts old .h5 model to work with newer Keras versions
"""

import os
import sys
from tensorflow import keras
from tensorflow.keras import losses, metrics

def fix_model(input_path, output_path=None):
    """
    Fix a Keras model by reloading and resaving with proper custom objects
    
    Args:
        input_path: Path to the problematic .h5 model
        output_path: Path to save fixed model (if None, overwrites input)
    """
    if output_path is None:
        output_path = input_path
        backup_path = input_path + '.backup'
        print(f"Creating backup at: {backup_path}")
        import shutil
        shutil.copy2(input_path, backup_path)
    
    print(f"Loading model from: {input_path}")
    
    try:
        # Try loading with custom objects
        custom_objects = {
            'mse': losses.MeanSquaredError(),
            'mae': metrics.MeanAbsoluteError(),
            'MeanSquaredError': losses.MeanSquaredError,
            'MeanAbsoluteError': metrics.MeanAbsoluteError
        }
        
        # Load without compiling
        model = keras.models.load_model(
            input_path,
            custom_objects=custom_objects,
            compile=False
        )
        
        print("✅ Model loaded successfully")
        print(f"   Input shape: {model.input_shape}")
        print(f"   Output shape: {model.output_shape}")
        
        # Recompile with proper objects
        print("\nRecompiling model...")
        model.compile(
            optimizer=keras.optimizers.Adam(learning_rate=0.001),
            loss=losses.MeanSquaredError(),
            metrics=[metrics.MeanAbsoluteError()]
        )
        
        print("✅ Model recompiled")
        
        # Save the fixed model
        print(f"\nSaving fixed model to: {output_path}")
        model.save(output_path, save_format='h5')
        
        print("✅ Model saved successfully")
        print("\n" + "="*60)
        print("SUCCESS! Model has been fixed and saved")
        print("="*60)
        
        if output_path == input_path:
            print(f"\nOriginal model backed up to: {backup_path}")
        
        return True
        
    except Exception as e:
        print(f"\n❌ Error fixing model: {e}")
        print(f"Error type: {type(e).__name__}")
        return False


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Fix Keras model for compatibility')
    parser.add_argument('input', help='Path to input .hd5 model')
    parser.add_argument('--output', '-o', help='Path to output .h5 model (optional)')
    
    args = parser.parse_args()
    
    if not os.path.exists(args.input):
        print(f"❌ Error: Model file not found: {args.input}")
        sys.exit(1)
    
    success = fix_model(args.input, args.output)
    sys.exit(0 if success else 1)
